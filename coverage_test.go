package respawn

import (
	"context"
	"errors"
	"testing"
	"time"
)

// instant replaces a Reconciler's sleep with a no-op so state-machine
// branches that would otherwise wait can be driven deterministically.
func instant(_ context.Context, _ time.Duration) error { return nil }

// --- pure helpers: exhaustive enum coverage ---------------------------

func TestState_StringAllStates(t *testing.T) {
	want := map[State]string{
		StateRunning:    "running",
		StateGraceWait:  "grace_wait",
		StateBackoff:    "backoff",
		StateRespawning: "respawning",
		StateCooldown:   "cooldown",
		StateStopped:    "stopped",
		State(99):       "running", // default arm
	}
	for s, w := range want {
		if got := s.String(); got != w {
			t.Errorf("State(%d).String() = %q ; want %q", int(s), got, w)
		}
	}
}

func TestKindName_All(t *testing.T) {
	want := map[SignalKind]string{
		SignalDown:      "down",
		SignalUnhealthy: "unhealthy",
		SignalUp:        "up",
		SignalHealthy:   "healthy",
		SignalKind(99):  "unknown",
	}
	for k, w := range want {
		if got := kindName(k); got != w {
			t.Errorf("kindName(%d) = %q ; want %q", int(k), got, w)
		}
	}
}

func TestActionName_All(t *testing.T) {
	want := map[Action]string{
		ActionStart: "start",
		ActionStop:  "stop",
		ActionWait:  "wait",
		ActionNone:  "none",
		Action(99):  "none",
	}
	for a, w := range want {
		if got := actionName(a); got != w {
			t.Errorf("actionName(%d) = %q ; want %q", int(a), got, w)
		}
	}
}

// --- decision helpers: remaining branches -----------------------------

func TestDecide_UpHealthyUnknownAreNoOp(t *testing.T) {
	policy := &RespawnPolicy{Enabled: true, MaxRestarts: 3, WindowMs: 60000}
	h := newHistory(3, time.Minute)
	now := time.Now()
	for _, k := range []SignalKind{SignalUp, SignalHealthy, SignalKind(99)} {
		plan := Decide(policy, h, 0, Signal{Kind: k}, now)
		if plan.Action != ActionNone || plan.State != StateRunning {
			t.Errorf("Decide(kind=%d) = %+v ; want running/none", int(k), plan)
		}
	}
}

func TestDecideAfterGrace_DisabledIsNoOp(t *testing.T) {
	plan := DecideAfterGrace(nil, newHistory(3, time.Minute), 0, SignalDown, time.Now())
	if plan.Action != ActionNone {
		t.Errorf("disabled DecideAfterGrace = %+v ; want none", plan)
	}
}

func TestDecideRespawn_ClampsNonPositiveCooldown(t *testing.T) {
	window := time.Second
	policy := &RespawnPolicy{Enabled: true, MaxRestarts: 2, WindowMs: window.Milliseconds()}
	now := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	h := newHistory(2, window)
	h.Append(now.Add(-window)) // exactly on the window edge → CooldownEnd == now
	h.Append(now.Add(-window / 2))
	plan := decideRespawn(policy, h, 2, SignalDown, now)
	if plan.State != StateCooldown {
		t.Fatalf("state = %v ; want cooldown", plan.State)
	}
	if plan.DelayFor != time.Second {
		t.Errorf("DelayFor = %v ; want 1s (clamped from <=0)", plan.DelayFor)
	}
}

func TestHistory_CooldownEndEmptyIsZero(t *testing.T) {
	if end := newHistory(3, time.Minute).CooldownEnd(); !end.IsZero() {
		t.Errorf("empty CooldownEnd = %v ; want zero", end)
	}
}

func TestVMState_StateAndHistoryAccessors(t *testing.T) {
	st := NewVMState(&RespawnPolicy{Enabled: true, MaxRestarts: 3, WindowMs: 60000})
	if st.State() != StateRunning {
		t.Errorf("initial State() = %v ; want running", st.State())
	}
	st.Apply(Plan{State: StateRunning, Action: ActionStart}, SignalDown, time.Now())
	if h := st.History(); len(h) != 1 {
		t.Errorf("History() len = %d ; want 1", len(h))
	}
}

// --- ctxSleep: all three branches -------------------------------------

func TestCtxSleep_Branches(t *testing.T) {
	if err := ctxSleep(context.Background(), 0); err != nil {
		t.Errorf("ctxSleep(0) = %v ; want nil", err)
	}
	if err := ctxSleep(context.Background(), time.Millisecond); err != nil {
		t.Errorf("ctxSleep(1ms) = %v ; want nil", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ctxSleep(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Errorf("ctxSleep(cancelled) = %v ; want context.Canceled", err)
	}
}

// --- handle: getState-nil, zero-When, ActionNone, grace, cooldown -----

func newReconcilerWithState(acts VMActions, vm string, policy *RespawnPolicy) (*Reconciler, *VMState) {
	r := New(acts, nil)
	r.sleep = instant
	st := NewVMState(policy)
	r.states[vm] = st
	return r, st
}

func TestHandle_UnwatchedIsNoOp(t *testing.T) {
	r := New(&fakeActions{}, nil)
	if err := r.handle(context.Background(), "ghost", Signal{VMName: "ghost", Kind: SignalDown}); err != nil {
		t.Errorf("handle(unwatched) = %v ; want nil", err)
	}
}

func TestHandle_ZeroWhenAndStartAction(t *testing.T) {
	acts := &fakeActions{}
	r, _ := newReconcilerWithState(acts, "v", &RespawnPolicy{Enabled: true, GracePeriodMs: 0, MaxRestarts: 5, WindowMs: 60000})
	// Zero When → handle stamps it with now.
	if err := r.handle(context.Background(), "v", Signal{VMName: "v", Kind: SignalDown}); err != nil {
		t.Fatalf("handle = %v", err)
	}
	if acts.startCount() != 1 {
		t.Errorf("StartVM calls = %d ; want 1", acts.startCount())
	}
}

func TestHandle_NoneActionOnUp(t *testing.T) {
	acts := &fakeActions{}
	r, _ := newReconcilerWithState(acts, "v", &RespawnPolicy{Enabled: true, MaxRestarts: 5, WindowMs: 60000})
	if err := r.handle(context.Background(), "v", Signal{VMName: "v", Kind: SignalUp, When: time.Now()}); err != nil {
		t.Fatalf("handle = %v", err)
	}
	if acts.startCount() != 0 {
		t.Errorf("SignalUp triggered %d starts ; want 0", acts.startCount())
	}
}

func TestHandle_GraceThenExecute(t *testing.T) {
	acts := &fakeActions{}
	r, _ := newReconcilerWithState(acts, "v", &RespawnPolicy{Enabled: true, GracePeriodMs: 100, MaxRestarts: 5, WindowMs: 60000})
	if err := r.handle(context.Background(), "v", Signal{VMName: "v", Kind: SignalDown, When: time.Now()}); err != nil {
		t.Fatalf("handle = %v", err)
	}
	// GraceWait → (instant sleep) → DecideAfterGrace → execute → StartVM.
	if acts.startCount() != 1 {
		t.Errorf("grace path StartVM = %d ; want 1", acts.startCount())
	}
}

func TestHandle_WaitSleepError(t *testing.T) {
	acts := &fakeActions{}
	r, _ := newReconcilerWithState(acts, "v", &RespawnPolicy{Enabled: true, GracePeriodMs: 100, MaxRestarts: 5, WindowMs: 60000})
	boom := errors.New("sleep interrupted")
	r.sleep = func(context.Context, time.Duration) error { return boom }
	if err := r.handle(context.Background(), "v", Signal{VMName: "v", Kind: SignalDown, When: time.Now()}); !errors.Is(err, boom) {
		t.Fatalf("handle = %v ; want sleep error", err)
	}
	if acts.startCount() != 0 {
		t.Errorf("interrupted grace should not Start: %d", acts.startCount())
	}
}

func TestHandle_CooldownWaitNonGrace(t *testing.T) {
	acts := &fakeActions{}
	// grace 0 + exhausted history → Decide returns a Cooldown ActionWait
	// whose State != GraceWait, exercising the non-grace ActionWait arm.
	policy := &RespawnPolicy{Enabled: true, GracePeriodMs: 0, MaxRestarts: 2, WindowMs: 60000}
	r, st := newReconcilerWithState(acts, "v", policy)
	now := time.Now()
	st.history.Append(now.Add(-1 * time.Second))
	st.history.Append(now.Add(-2 * time.Second))
	if err := r.handle(context.Background(), "v", Signal{VMName: "v", Kind: SignalDown, When: now}); err != nil {
		t.Fatalf("handle = %v", err)
	}
	if acts.startCount() != 0 {
		t.Errorf("cooldown should not Start: %d", acts.startCount())
	}
	if st.State() != StateCooldown {
		t.Errorf("state = %v ; want cooldown", st.State())
	}
}

// --- execute: error + backoff branches --------------------------------

func TestExecute_StartError(t *testing.T) {
	boom := errors.New("start boom")
	acts := &fakeActions{startErr: boom}
	r, st := newReconcilerWithState(acts, "v", &RespawnPolicy{Enabled: true, MaxRestarts: 5, WindowMs: 60000})
	err := r.execute(context.Background(), "v", st, Plan{State: StateRespawning, Action: ActionStart}, SignalDown)
	if !errors.Is(err, boom) {
		t.Fatalf("execute = %v ; want start boom", err)
	}
}

func TestExecute_StopError(t *testing.T) {
	boom := errors.New("stop boom")
	acts := &fakeActions{stopErr: boom}
	r, st := newReconcilerWithState(acts, "v", &RespawnPolicy{Enabled: true, MaxRestarts: 5, WindowMs: 60000})
	err := r.execute(context.Background(), "v", st, Plan{State: StateRespawning, Action: ActionStop}, SignalUnhealthy)
	if !errors.Is(err, boom) {
		t.Fatalf("execute = %v ; want stop boom", err)
	}
}

func TestExecute_PostStopStartError(t *testing.T) {
	boom := errors.New("post-stop start boom")
	acts := &fakeActions{startErr: boom} // Stop ok, chained Start fails
	r, st := newReconcilerWithState(acts, "v", &RespawnPolicy{Enabled: true, MaxRestarts: 5, WindowMs: 60000})
	err := r.execute(context.Background(), "v", st, Plan{State: StateRespawning, Action: ActionStop}, SignalUnhealthy)
	if !errors.Is(err, boom) {
		t.Fatalf("execute = %v ; want post-stop start boom", err)
	}
}

func TestExecute_BackoffThenStart(t *testing.T) {
	acts := &fakeActions{}
	r, st := newReconcilerWithState(acts, "v", &RespawnPolicy{Enabled: true, MaxRestarts: 5, WindowMs: 60000})
	// DelayFor > 0 exercises the backoff sleep branch (instant seam).
	err := r.execute(context.Background(), "v", st, Plan{State: StateBackoff, Action: ActionStart, DelayFor: time.Second}, SignalDown)
	if err != nil {
		t.Fatalf("execute = %v ; want nil", err)
	}
	if acts.startCount() != 1 {
		t.Errorf("StartVM after backoff = %d ; want 1", acts.startCount())
	}
}

func TestExecute_BackoffSleepInterrupted(t *testing.T) {
	acts := &fakeActions{}
	r, st := newReconcilerWithState(acts, "v", &RespawnPolicy{Enabled: true, MaxRestarts: 5, WindowMs: 60000})
	boom := errors.New("backoff cancelled")
	r.sleep = func(context.Context, time.Duration) error { return boom }
	err := r.execute(context.Background(), "v", st, Plan{State: StateBackoff, Action: ActionStart, DelayFor: time.Second}, SignalDown)
	if !errors.Is(err, boom) {
		t.Fatalf("execute = %v ; want backoff cancelled", err)
	}
	if acts.startCount() != 0 {
		t.Errorf("interrupted backoff should not Start: %d", acts.startCount())
	}
}
