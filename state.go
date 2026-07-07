// Package respawn implements a restart/reconcile state machine that
// honours a RespawnPolicy : it watches a named unit (fed a stream of
// down / up / unhealthy / healthy Signals), decides when to respawn
// after a death signal, and enforces the anti-thrash limits
// (max_restarts inside window_ms, optional exponential backoff).
//
// The package is deliberately dependency-free and decoupled from any
// transport or health checker : RespawnPolicy is a local value type,
// health is expressed as a local SignalKind (not an external probe
// type), and the Reconciler effects respawns through a caller-supplied
// Actions interface. A unit test can drive the whole machine with a
// fixed clock and a fake Actions, without standing up anything real.
package respawn

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// RespawnPolicy declares automatic recovery rules for a supervised unit.
// It is a local, dependency-free value type — the fields the state
// machine consumes, inlined so this package depends on no external
// schema. The Get* accessors are nil-safe (a nil *RespawnPolicy reads as
// the zero policy), mirroring generated-accessor semantics so callers
// may pass nil to mean "no policy".
type RespawnPolicy struct {
	// Enabled turns respawn on. A disabled (or nil) policy is a no-op.
	Enabled bool
	// GracePeriodMs waits after the first "down" signal before
	// respawning, to debounce transient flaps.
	GracePeriodMs int64
	// MaxRestarts caps restarts within WindowMs (anti-thrash).
	MaxRestarts int32
	// WindowMs is the sliding window MaxRestarts is counted over.
	WindowMs int64
	// Backoff selects the retry delay curve: "constant" or "exponential"
	// (the empty string behaves as "constant").
	Backoff string
	// InitialDelayMs is the initial backoff delay; it doubles each retry
	// when Backoff is "exponential".
	InitialDelayMs int64
}

// GetEnabled reports whether respawn is enabled (false for a nil policy).
func (p *RespawnPolicy) GetEnabled() bool {
	if p == nil {
		return false
	}
	return p.Enabled
}

// GetGracePeriodMs returns the grace period in ms (0 for a nil policy).
func (p *RespawnPolicy) GetGracePeriodMs() int64 {
	if p == nil {
		return 0
	}
	return p.GracePeriodMs
}

// GetMaxRestarts returns the restart cap (0 for a nil policy).
func (p *RespawnPolicy) GetMaxRestarts() int32 {
	if p == nil {
		return 0
	}
	return p.MaxRestarts
}

// GetWindowMs returns the sliding window in ms (0 for a nil policy).
func (p *RespawnPolicy) GetWindowMs() int64 {
	if p == nil {
		return 0
	}
	return p.WindowMs
}

// GetBackoff returns the backoff curve name ("" for a nil policy).
func (p *RespawnPolicy) GetBackoff() string {
	if p == nil {
		return ""
	}
	return p.Backoff
}

// GetInitialDelayMs returns the initial backoff delay in ms (0 for nil).
func (p *RespawnPolicy) GetInitialDelayMs() int64 {
	if p == nil {
		return 0
	}
	return p.InitialDelayMs
}

// State is where one watched VM sits in the respawn state machine.
//
//	Running   - VM is up, no down signal observed
//	GraceWait - down signal observed ; waiting grace_period_ms to debounce flaps
//	Backoff   - grace expired ; sleeping backoff before issuing StartVM
//	Respawning- StartVM in flight
//	Cooldown  - max_restarts exhausted in current window ; waiting for the
//	            window to roll before respawning again
//	Stopped   - controller told us to stop watching this VM (rule removed,
//	            VM deleted, ctx cancelled). Terminal.
type State int

const (
	StateRunning State = iota
	StateGraceWait
	StateBackoff
	StateRespawning
	StateCooldown
	StateStopped
)

func (s State) String() string {
	switch s {
	case StateGraceWait:
		return "grace_wait"
	case StateBackoff:
		return "backoff"
	case StateRespawning:
		return "respawning"
	case StateCooldown:
		return "cooldown"
	case StateStopped:
		return "stopped"
	default:
		return "running"
	}
}

// SignalKind is the input the reconciler reacts to.
type SignalKind int

const (
	// SignalDown : the VM stopped (operator action, kernel panic,
	// scheduler eviction, host reboot, …). Surfaced by the agent's
	// VM state stream.
	SignalDown SignalKind = iota
	// SignalUp : the VM came back to "running". Used to exit
	// Respawning state and reset transient counters.
	SignalUp
	// SignalUnhealthy : a Probe (HTTP/TCP/EXEC) has gone unhealthy
	// past failure_threshold. Treated as a soft down — same flow
	// but the VM is still nominally running, so respawn means
	// "stop + start" not just "start".
	SignalUnhealthy
	// SignalHealthy : the rolling probe verdict crossed back to
	// healthy. Resets transient unhealthy state.
	SignalHealthy
)

// Signal is one input to the reconciler. VMName identifies which VM
// the signal is about — one Reconciler instance can watch many VMs.
type Signal struct {
	VMName string
	Kind   SignalKind
	When   time.Time
}

// VMActions is the side-effect surface the reconciler calls into to
// effect a respawn. The agent supplies an impl that wraps its own
// VM driver dispatch ; tests supply a fake to assert state-machine
// behaviour without touching the hypervisor.
//
// StartVM is the V0.1 happy path (VM died → bring it back). StopVM
// is called before StartVM when the trigger was an Unhealthy signal
// against an otherwise-running VM (the policy doesn't trust the
// guest enough to recover on its own — it gets restarted).
type VMActions interface {
	StartVM(ctx context.Context, name string) error
	StopVM(ctx context.Context, name string) error
}

// History is the rolling restart log per VM, capped at the policy's
// max_restarts. Each entry is a Time the StartVM was issued ; the
// window check at decide() drops entries older than window_ms before
// counting.
//
// mu protects entries against the brief Unwatch-then-Watch overlap : a
// signal already in-flight inside the OLD per-VM goroutine can race
// the freshly-spawned NEW goroutine's handle() if the operator
// replaces the rule between two bursts. Both paths look up the same
// VMState through r.states[vmName], so we serialise the read+write.
type History struct {
	mu      sync.Mutex
	entries []time.Time
	max     int
	window  time.Duration
}

func newHistory(max int, window time.Duration) *History {
	if max <= 0 {
		max = 5
	}
	if window <= 0 {
		window = 10 * time.Minute
	}
	return &History{max: max, window: window}
}

// Append records a restart at now and returns whether the policy is
// now exhausted (i.e. count-in-window >= max). Callers use the result
// to transition into Cooldown.
func (h *History) Append(now time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.trimLocked(now)
	h.entries = append(h.entries, now)
	return len(h.entries) >= h.max
}

// Exhausted reports whether a respawn at `now` would breach
// max_restarts in the current window. Equivalent to Append's return
// value without actually mutating the log.
func (h *History) Exhausted(now time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.trimLocked(now)
	return len(h.entries) >= h.max
}

// CooldownEnd returns the time at which the oldest in-window entry
// rolls off — the earliest moment a respawn would be permitted again.
// Returns zero when the history is empty.
func (h *History) CooldownEnd() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.entries) == 0 {
		return time.Time{}
	}
	return h.entries[0].Add(h.window)
}

// trimLocked drops entries older than now-window. Caller must hold h.mu.
func (h *History) trimLocked(now time.Time) {
	cutoff := now.Add(-h.window)
	i := 0
	for ; i < len(h.entries); i++ {
		if !h.entries[i].Before(cutoff) {
			break
		}
	}
	if i > 0 {
		h.entries = append([]time.Time(nil), h.entries[i:]...)
	}
}

// Plan describes the action the state machine has decided on for one
// VM at one point in time. The reconciler converts a Signal into a
// Plan, then executes it via VMActions. Keeping Plan a pure value
// makes the decision logic unit-testable without time/goroutines.
type Plan struct {
	State    State
	Action   Action
	DelayFor time.Duration // how long to wait before next attempt (Backoff/Cooldown)
	Reason   string
}

type Action int

const (
	ActionNone  Action = iota
	ActionStart        // StartVM (used after Down signal)
	ActionStop         // StopVM (used after Unhealthy, before Start)
	ActionWait         // park in current state for DelayFor (backoff/grace/cooldown)
)

// Decide is the pure function at the heart of the state machine. It
// takes the policy + the per-VM History + an incoming signal at time
// now, and emits the Plan the reconciler executes.
//
// The function is deliberately allocation-free apart from the returned
// Plan. All time arithmetic is computed against `now` so tests pass a
// fixed clock and assert exact deadlines.
func Decide(policy *RespawnPolicy, history *History, attempt int, sig Signal, now time.Time) Plan {
	if policy == nil || !policy.GetEnabled() {
		return Plan{State: StateRunning, Action: ActionNone, Reason: "respawn disabled"}
	}
	switch sig.Kind {
	case SignalDown, SignalUnhealthy:
		// Grace period — let a transient flap settle before reacting.
		grace := time.Duration(policy.GetGracePeriodMs()) * time.Millisecond
		if grace > 0 {
			return Plan{
				State:    StateGraceWait,
				Action:   ActionWait,
				DelayFor: grace,
				Reason:   fmt.Sprintf("waiting grace_period %s after %s", grace, kindName(sig.Kind)),
			}
		}
		return decideRespawn(policy, history, attempt, sig.Kind, now)
	case SignalUp:
		return Plan{State: StateRunning, Action: ActionNone, Reason: "VM up"}
	case SignalHealthy:
		return Plan{State: StateRunning, Action: ActionNone, Reason: "probe healthy"}
	default:
		return Plan{State: StateRunning, Action: ActionNone, Reason: "unknown signal"}
	}
}

// DecideAfterGrace is the post-debounce decision : grace_period
// elapsed, the down condition is still true, time to respawn.
func DecideAfterGrace(policy *RespawnPolicy, history *History, attempt int, kind SignalKind, now time.Time) Plan {
	if policy == nil || !policy.GetEnabled() {
		return Plan{State: StateRunning, Action: ActionNone, Reason: "respawn disabled"}
	}
	return decideRespawn(policy, history, attempt, kind, now)
}

func decideRespawn(policy *RespawnPolicy, history *History, attempt int, kind SignalKind, now time.Time) Plan {
	if history.Exhausted(now) {
		end := history.CooldownEnd()
		wait := end.Sub(now)
		if wait <= 0 {
			wait = time.Second
		}
		return Plan{
			State:    StateCooldown,
			Action:   ActionWait,
			DelayFor: wait,
			Reason:   fmt.Sprintf("max_restarts=%d exhausted in window ; cooldown until %s", policy.GetMaxRestarts(), end.Format(time.RFC3339)),
		}
	}
	backoff := computeBackoff(policy, attempt)
	action := ActionStart
	if kind == SignalUnhealthy {
		action = ActionStop // caller chains Stop→Start
	}
	st := StateBackoff
	if backoff == 0 {
		st = StateRespawning
	}
	return Plan{
		State:    st,
		Action:   action,
		DelayFor: backoff,
		Reason:   fmt.Sprintf("respawning after %s (attempt %d, backoff %s)", kindName(kind), attempt+1, backoff),
	}
}

// computeBackoff returns the wait before the next respawn attempt.
// "constant" : initial_delay_ms (or 0).
// "exponential" : initial_delay_ms * 2^attempt, capped at 5 min.
func computeBackoff(policy *RespawnPolicy, attempt int) time.Duration {
	base := time.Duration(policy.GetInitialDelayMs()) * time.Millisecond
	if base <= 0 {
		return 0
	}
	switch policy.GetBackoff() {
	case "exponential":
		const ceil = 5 * time.Minute
		// shift but clamp : attempt > 30 would overflow.
		shift := attempt
		if shift > 30 {
			shift = 30
		}
		d := base << shift
		if d > ceil || d < 0 {
			return ceil
		}
		return d
	default: // "" and "constant" both behave the same
		return base
	}
}

func kindName(k SignalKind) string {
	switch k {
	case SignalDown:
		return "down"
	case SignalUnhealthy:
		return "unhealthy"
	case SignalUp:
		return "up"
	case SignalHealthy:
		return "healthy"
	}
	return "unknown"
}

// VMState tracks per-VM bookkeeping in the Reconciler. Locked
// individually so concurrent signals for distinct VMs don't serialise.
type VMState struct {
	mu       sync.Mutex
	state    State
	attempts int
	history  *History
	policy   *RespawnPolicy
}

// NewVMState builds a VMState for the given policy.
func NewVMState(policy *RespawnPolicy) *VMState {
	return &VMState{
		state:   StateRunning,
		policy:  policy,
		history: newHistory(int(policy.GetMaxRestarts()), time.Duration(policy.GetWindowMs())*time.Millisecond),
	}
}

// State returns the current state. Safe to call from any goroutine.
func (v *VMState) State() State {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.state
}

// Attempts returns how many respawns have happened. Useful for
// observability.
func (v *VMState) Attempts() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.attempts
}

// Apply records a Plan + signal as having been executed. The
// reconciler calls this after a successful StartVM so the History
// is updated atomically with the state.
func (v *VMState) Apply(plan Plan, kind SignalKind, now time.Time) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.state = plan.State
	if plan.Action == ActionStart || plan.Action == ActionStop {
		v.history.Append(now)
		v.attempts++
	}
}

// History returns a copy of the underlying log entries. Useful for
// observability / metrics ; doesn't expose the slice header so a
// caller can't mutate the internal store.
func (v *VMState) History() []time.Time {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]time.Time, len(v.history.entries))
	copy(out, v.history.entries)
	return out
}
