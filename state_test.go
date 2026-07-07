package respawn

import (
	"testing"
	"time"
)

func TestDecide_DisabledIsNoOp(t *testing.T) {
	plan := Decide(&RespawnPolicy{Enabled: false}, newHistory(5, time.Minute), 0,
		Signal{Kind: SignalDown}, time.Now())
	if plan.Action != ActionNone {
		t.Errorf("disabled policy: Action=%v ; want None", plan.Action)
	}
}

func TestDecide_GraceWaitFiresBeforeRespawn(t *testing.T) {
	policy := &RespawnPolicy{
		Enabled: true, GracePeriodMs: 5000, MaxRestarts: 3, WindowMs: 60000,
	}
	plan := Decide(policy, newHistory(3, time.Minute), 0,
		Signal{Kind: SignalDown}, time.Now())
	if plan.State != StateGraceWait || plan.Action != ActionWait {
		t.Errorf("got state=%v action=%v ; want grace_wait/wait", plan.State, plan.Action)
	}
	if plan.DelayFor != 5*time.Second {
		t.Errorf("DelayFor=%v ; want 5s", plan.DelayFor)
	}
}

func TestDecide_NoGraceGoesStraightToRespawn(t *testing.T) {
	policy := &RespawnPolicy{
		Enabled: true, GracePeriodMs: 0, MaxRestarts: 3, WindowMs: 60000,
	}
	plan := Decide(policy, newHistory(3, time.Minute), 0,
		Signal{Kind: SignalDown}, time.Now())
	if plan.Action != ActionStart {
		t.Errorf("no grace: Action=%v ; want Start", plan.Action)
	}
}

func TestDecide_UnhealthyTriggersStop(t *testing.T) {
	policy := &RespawnPolicy{Enabled: true, MaxRestarts: 3, WindowMs: 60000}
	plan := DecideAfterGrace(policy, newHistory(3, time.Minute), 0, SignalUnhealthy, time.Now())
	if plan.Action != ActionStop {
		t.Errorf("unhealthy → Action=%v ; want Stop (then chained Start)", plan.Action)
	}
}

func TestDecide_ExhaustedHistoryCoolsDown(t *testing.T) {
	// max_restarts=3 in 1min window ; pre-fill the history with 3
	// recent entries so a new respawn would breach the cap.
	policy := &RespawnPolicy{Enabled: true, MaxRestarts: 3, WindowMs: 60000}
	now := time.Now()
	h := newHistory(3, time.Minute)
	h.Append(now.Add(-30 * time.Second))
	h.Append(now.Add(-20 * time.Second))
	h.Append(now.Add(-10 * time.Second))
	plan := DecideAfterGrace(policy, h, 3, SignalDown, now)
	if plan.State != StateCooldown {
		t.Errorf("State=%v ; want Cooldown", plan.State)
	}
	if plan.Action != ActionWait {
		t.Errorf("Action=%v ; want Wait", plan.Action)
	}
	// CooldownEnd of oldest entry (-30s) + 60s window = +30s ahead.
	wantDelay := 30 * time.Second
	if d := plan.DelayFor; d < wantDelay-time.Second || d > wantDelay+time.Second {
		t.Errorf("DelayFor=%v ; want ~%v", d, wantDelay)
	}
}

func TestHistory_TrimsOldEntries(t *testing.T) {
	h := newHistory(5, 10*time.Second)
	base := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	h.Append(base)
	h.Append(base.Add(2 * time.Second))
	// Move forward 30s — both entries should fall out of the window.
	// trimLocked is unexported ; call it through Append which trims
	// before appending, then verify only the new entry remains in
	// the post-window region.
	h.Append(base.Add(30 * time.Second))
	if len(h.entries) != 1 {
		t.Errorf("after window expiry + 1 new Append, h.entries = %d ; want 1 (the new entry)", len(h.entries))
	}
}

func TestBackoff_ConstantStaysAtInitial(t *testing.T) {
	policy := &RespawnPolicy{InitialDelayMs: 1000, Backoff: "constant"}
	for attempt := 0; attempt < 5; attempt++ {
		got := computeBackoff(policy, attempt)
		if got != time.Second {
			t.Errorf("attempt %d : got %v ; want 1s", attempt, got)
		}
	}
}

func TestBackoff_ExponentialDoubles(t *testing.T) {
	policy := &RespawnPolicy{InitialDelayMs: 100, Backoff: "exponential"}
	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
	}
	for i, w := range want {
		if got := computeBackoff(policy, i); got != w {
			t.Errorf("attempt %d : got %v ; want %v", i, got, w)
		}
	}
}

func TestBackoff_ExponentialCapsAt5Min(t *testing.T) {
	policy := &RespawnPolicy{InitialDelayMs: 1000, Backoff: "exponential"}
	got := computeBackoff(policy, 100)
	if got != 5*time.Minute {
		t.Errorf("attempt 100 : got %v ; want 5m cap", got)
	}
}

func TestVMState_AppliedActionsTickHistory(t *testing.T) {
	policy := &RespawnPolicy{Enabled: true, MaxRestarts: 5, WindowMs: 60000}
	st := NewVMState(policy)
	if st.Attempts() != 0 {
		t.Errorf("initial Attempts() = %d ; want 0", st.Attempts())
	}
	st.Apply(Plan{State: StateRunning, Action: ActionStart}, SignalDown, time.Now())
	if st.Attempts() != 1 {
		t.Errorf("after one Start: Attempts() = %d ; want 1", st.Attempts())
	}
	// ActionNone shouldn't tick the counter.
	st.Apply(Plan{State: StateRunning, Action: ActionNone}, SignalUp, time.Now())
	if st.Attempts() != 1 {
		t.Errorf("after None: Attempts() = %d ; want 1 (unchanged)", st.Attempts())
	}
}

func TestRespawnPolicy_NilGettersAreZero(t *testing.T) {
	var p *RespawnPolicy
	if p.GetEnabled() {
		t.Error("nil GetEnabled = true ; want false")
	}
	if p.GetGracePeriodMs() != 0 {
		t.Error("nil GetGracePeriodMs != 0")
	}
	if p.GetMaxRestarts() != 0 {
		t.Error("nil GetMaxRestarts != 0")
	}
	if p.GetWindowMs() != 0 {
		t.Error("nil GetWindowMs != 0")
	}
	if p.GetBackoff() != "" {
		t.Error("nil GetBackoff != \"\"")
	}
	if p.GetInitialDelayMs() != 0 {
		t.Error("nil GetInitialDelayMs != 0")
	}
}
