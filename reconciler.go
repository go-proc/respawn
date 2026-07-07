package respawn

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Reconciler is the long-running side of the package : it consumes
// Signals from a channel, holds a per-VM state machine, and calls
// VMActions to effect respawns. One Reconciler instance per agent
// is enough — it fans out across VMs by name.
//
// Concurrency :
//   - one goroutine per VM the reconciler is watching, started
//     lazily on the first signal for that VM.
//   - Stop() drains by closing the input channel ; per-VM goroutines
//     exit when ctx is cancelled.
type Reconciler struct {
	actions VMActions
	log     *slog.Logger
	now     func() time.Time
	sleep   func(context.Context, time.Duration) error

	mu     sync.Mutex
	states map[string]*VMState
	chans  map[string]chan Signal

	wg sync.WaitGroup
}

// New builds a Reconciler that uses actions for side effects. log
// can be nil ; a discard handler is substituted.
func New(actions VMActions, log *slog.Logger) *Reconciler {
	if log == nil {
		log = slog.New(slog.NewTextHandler(discardWriter{}, &slog.HandlerOptions{Level: slog.LevelError}))
	}
	return &Reconciler{
		actions: actions,
		log:     log,
		now:     time.Now,
		sleep:   ctxSleep,
		states:  make(map[string]*VMState),
		chans:   make(map[string]chan Signal),
	}
}

type discardWriter struct{}

func (discardWriter) Write(b []byte) (int, error) { return len(b), nil }

func ctxSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Watch starts (or replaces) the per-VM goroutine for vmName under
// policy. Subsequent Watch calls for the same vmName replace the
// policy without dropping the goroutine ; signals delivered between
// the two policy versions are processed under the old policy.
func (r *Reconciler) Watch(ctx context.Context, vmName string, policy *RespawnPolicy) {
	r.mu.Lock()
	if _, ok := r.chans[vmName]; ok {
		// Update policy in place ; existing goroutine keeps running
		// and picks up the new policy on next signal.
		r.states[vmName] = NewVMState(policy)
		r.mu.Unlock()
		return
	}
	ch := make(chan Signal, 16)
	r.chans[vmName] = ch
	r.states[vmName] = NewVMState(policy)
	r.mu.Unlock()

	r.wg.Add(1)
	go r.loop(ctx, vmName, ch)
}

// Unwatch stops watching vmName. The per-VM goroutine exits cleanly ;
// future Signal() calls for vmName are dropped silently (no panic).
func (r *Reconciler) Unwatch(vmName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ch, ok := r.chans[vmName]; ok {
		close(ch)
		delete(r.chans, vmName)
		delete(r.states, vmName)
	}
}

// Send injects a signal for vmName. Non-blocking ; if the per-VM
// channel is full the signal is dropped + logged (the reconciler's
// channel buffer is sized 16, enough for the bursts a healthy agent
// produces).
func (r *Reconciler) Send(sig Signal) {
	// Hold mu around the non-blocking send : a concurrent Unwatch
	// closes ch + deletes the map entry under the same mu, so without
	// the lock the lookup could return a not-yet-deleted ch that's
	// already closed by the next instruction → send-on-closed panic.
	r.mu.Lock()
	defer r.mu.Unlock()
	ch, ok := r.chans[sig.VMName]
	if !ok {
		return
	}
	select {
	case ch <- sig:
	default:
		r.log.Warn("respawn signal dropped (channel full)", "vm", sig.VMName, "kind", kindName(sig.Kind))
	}
}

// Wait blocks until all per-VM goroutines have exited. Call after
// cancelling the parent context.
func (r *Reconciler) Wait() { r.wg.Wait() }

// loop is the per-VM goroutine. Consumes Signals + drives the
// state machine.
func (r *Reconciler) loop(ctx context.Context, vmName string, ch <-chan Signal) {
	defer r.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case sig, ok := <-ch:
			if !ok {
				return
			}
			if err := r.handle(ctx, vmName, sig); err != nil {
				r.log.Error("respawn handle failed", "vm", vmName, "err", err)
			}
		}
	}
}

func (r *Reconciler) handle(ctx context.Context, vmName string, sig Signal) error {
	st := r.getState(vmName)
	if st == nil {
		return nil // unwatched between Send + handle
	}
	now := r.now()
	if sig.When.IsZero() {
		sig.When = now
	}
	policy, attempts := r.snapshot(st)
	plan := Decide(policy, st.history, attempts, sig, now)
	r.log.Info("respawn plan",
		"vm", vmName, "signal", kindName(sig.Kind), "state", plan.State.String(),
		"action", actionName(plan.Action), "delay", plan.DelayFor, "reason", plan.Reason,
	)

	switch plan.Action {
	case ActionNone:
		st.Apply(plan, sig.Kind, now)
		return nil
	case ActionWait:
		st.Apply(plan, sig.Kind, now)
		if err := r.sleep(ctx, plan.DelayFor); err != nil {
			return err
		}
		// After the wait elapses, re-evaluate against the same
		// signal kind — if the down condition is still true the
		// caller should keep us here. For grace_wait, that means
		// proceeding to backoff/respawning.
		if plan.State == StateGraceWait {
			afterPlan := DecideAfterGrace(policy, st.history, attempts, sig.Kind, r.now())
			r.log.Info("respawn after grace",
				"vm", vmName, "state", afterPlan.State.String(),
				"action", actionName(afterPlan.Action), "delay", afterPlan.DelayFor,
			)
			return r.execute(ctx, vmName, st, afterPlan, sig.Kind)
		}
		return nil
	default: // ActionStart, ActionStop
		return r.execute(ctx, vmName, st, plan, sig.Kind)
	}
}

func (r *Reconciler) execute(ctx context.Context, vmName string, st *VMState, plan Plan, kind SignalKind) error {
	// Backoff first, if any.
	if plan.DelayFor > 0 {
		if err := r.sleep(ctx, plan.DelayFor); err != nil {
			return err
		}
	}
	// Record the attempt in history BEFORE the actual side effect, so
	// concurrent decisions see the bumped count and honour
	// max_restarts even when the StartVM call takes nontrivial time.
	st.Apply(plan, kind, r.now())
	switch plan.Action {
	case ActionStop:
		if err := r.actions.StopVM(ctx, vmName); err != nil {
			r.log.Error("StopVM failed", "vm", vmName, "err", err)
			return err
		}
		// Chain Start after Stop for Unhealthy case.
		if err := r.actions.StartVM(ctx, vmName); err != nil {
			r.log.Error("StartVM (post-stop) failed", "vm", vmName, "err", err)
			return err
		}
	case ActionStart:
		if err := r.actions.StartVM(ctx, vmName); err != nil {
			r.log.Error("StartVM failed", "vm", vmName, "err", err)
			return err
		}
	}
	// Settle back to Running with a no-op Apply (won't tick history).
	st.Apply(Plan{State: StateRunning, Action: ActionNone}, SignalUp, r.now())
	return nil
}

func (r *Reconciler) getState(vmName string) *VMState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.states[vmName]
}

func (r *Reconciler) snapshot(st *VMState) (*RespawnPolicy, int) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.policy, st.attempts
}

func actionName(a Action) string {
	switch a {
	case ActionStart:
		return "start"
	case ActionStop:
		return "stop"
	case ActionWait:
		return "wait"
	}
	return "none"
}
