package respawn

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeActions struct {
	mu        sync.Mutex
	starts    []string
	stops     []string
	startErr  error
	stopErr   error
	startWait time.Duration
}

func (f *fakeActions) StartVM(_ context.Context, name string) error {
	if f.startWait > 0 {
		time.Sleep(f.startWait)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts = append(f.starts, name)
	return f.startErr
}

func (f *fakeActions) StopVM(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stops = append(f.stops, name)
	return f.stopErr
}

func (f *fakeActions) startCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.starts)
}

func TestReconciler_RestartsVMOnDownSignal(t *testing.T) {
	acts := &fakeActions{}
	r := New(acts, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	policy := &RespawnPolicy{
		Enabled: true, GracePeriodMs: 0, MaxRestarts: 5, WindowMs: 60000,
	}
	r.Watch(ctx, "vm-1", policy)
	r.Send(Signal{VMName: "vm-1", Kind: SignalDown, When: time.Now()})
	waitFor(t, time.Second, func() bool { return acts.startCount() >= 1 })

	if acts.startCount() != 1 {
		t.Errorf("got %d StartVM calls ; want 1", acts.startCount())
	}
	if got := acts.starts[0]; got != "vm-1" {
		t.Errorf("StartVM(%q) ; want vm-1", got)
	}
}

func TestReconciler_RespectsGracePeriod(t *testing.T) {
	acts := &fakeActions{}
	r := New(acts, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	policy := &RespawnPolicy{
		Enabled: true, GracePeriodMs: 200, MaxRestarts: 5, WindowMs: 60000,
	}
	r.Watch(ctx, "vm-grace", policy)
	r.Send(Signal{VMName: "vm-grace", Kind: SignalDown, When: time.Now()})

	// Before grace elapses, no Start.
	time.Sleep(80 * time.Millisecond)
	if acts.startCount() != 0 {
		t.Errorf("StartVM fired before grace : %d calls", acts.startCount())
	}
	// After grace + a little slack, Start fires.
	waitFor(t, 2*time.Second, func() bool { return acts.startCount() >= 1 })
}

func TestReconciler_ChainsStopAndStartOnUnhealthy(t *testing.T) {
	acts := &fakeActions{}
	r := New(acts, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	policy := &RespawnPolicy{
		Enabled: true, GracePeriodMs: 0, MaxRestarts: 5, WindowMs: 60000,
	}
	r.Watch(ctx, "vm-sick", policy)
	r.Send(Signal{VMName: "vm-sick", Kind: SignalUnhealthy, When: time.Now()})
	waitFor(t, time.Second, func() bool { return acts.startCount() >= 1 })

	acts.mu.Lock()
	defer acts.mu.Unlock()
	if len(acts.stops) != 1 || acts.stops[0] != "vm-sick" {
		t.Errorf("stops=%v ; want [vm-sick]", acts.stops)
	}
	if len(acts.starts) != 1 || acts.starts[0] != "vm-sick" {
		t.Errorf("starts=%v ; want [vm-sick]", acts.starts)
	}
}

func TestReconciler_UnwatchStopsSignals(t *testing.T) {
	acts := &fakeActions{}
	r := New(acts, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	policy := &RespawnPolicy{Enabled: true, MaxRestarts: 5, WindowMs: 60000}
	r.Watch(ctx, "vm-gone", policy)
	r.Unwatch("vm-gone")
	// Should silently drop, no panic.
	r.Send(Signal{VMName: "vm-gone", Kind: SignalDown, When: time.Now()})
	time.Sleep(50 * time.Millisecond)
	if acts.startCount() != 0 {
		t.Errorf("unwatched VM still got StartVM : %d calls", acts.startCount())
	}
}

func TestReconciler_HonoursMaxRestarts(t *testing.T) {
	acts := &fakeActions{}
	r := New(acts, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	policy := &RespawnPolicy{
		Enabled: true, GracePeriodMs: 0, MaxRestarts: 2, WindowMs: 60000,
	}
	r.Watch(ctx, "vm-thrash", policy)
	for i := 0; i < 5; i++ {
		r.Send(Signal{VMName: "vm-thrash", Kind: SignalDown, When: time.Now()})
		// Tiny sleep so each handle() finishes before the next signal lands.
		time.Sleep(30 * time.Millisecond)
	}
	// Should have started at most max_restarts=2 times. The third+ go
	// into Cooldown, which makes the goroutine sleep for ~window — we
	// don't wait for it ; just assert the cap.
	if got := acts.startCount(); got > 2 {
		t.Errorf("Start fired %d times ; want ≤2 (max_restarts cap)", got)
	}
}

func TestReconciler_PolicyUpdateReplacesPolicy(t *testing.T) {
	acts := &fakeActions{}
	r := New(acts, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r.Watch(ctx, "vm-update", &RespawnPolicy{Enabled: false})
	r.Send(Signal{VMName: "vm-update", Kind: SignalDown, When: time.Now()})
	time.Sleep(50 * time.Millisecond)
	if acts.startCount() != 0 {
		t.Errorf("disabled policy fired Start : %d calls", acts.startCount())
	}

	// Flip to enabled — second Watch is the update path.
	r.Watch(ctx, "vm-update", &RespawnPolicy{
		Enabled: true, GracePeriodMs: 0, MaxRestarts: 5, WindowMs: 60000,
	})
	r.Send(Signal{VMName: "vm-update", Kind: SignalDown, When: time.Now()})
	waitFor(t, time.Second, func() bool { return acts.startCount() >= 1 })
}

func TestReconciler_ConcurrentVMsDontSerialise(t *testing.T) {
	acts := &fakeActions{startWait: 100 * time.Millisecond}
	r := New(acts, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	policy := &RespawnPolicy{
		Enabled: true, GracePeriodMs: 0, MaxRestarts: 5, WindowMs: 60000,
	}
	const N = 5
	for i := 0; i < N; i++ {
		r.Watch(ctx, vmName(i), policy)
	}
	start := time.Now()
	for i := 0; i < N; i++ {
		r.Send(Signal{VMName: vmName(i), Kind: SignalDown, When: time.Now()})
	}
	waitFor(t, 2*time.Second, func() bool { return acts.startCount() >= N })
	elapsed := time.Since(start)
	// If they serialised, this would take 5×100ms = 500ms. Allow
	// ample slack ; we just want to confirm it's well under that.
	if elapsed > 350*time.Millisecond {
		t.Errorf("N VMs took %v ; serialised? (want <350ms)", elapsed)
	}
}

func vmName(i int) string { return "vm-" + string(rune('a'+i)) }

func waitFor(t *testing.T, max time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(max)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition never reached")
}

func TestReconciler_ContextCancelStopsLoops(t *testing.T) {
	acts := &fakeActions{}
	r := New(acts, nil)
	ctx, cancel := context.WithCancel(context.Background())
	policy := &RespawnPolicy{Enabled: true, MaxRestarts: 5, WindowMs: 60000}
	r.Watch(ctx, "vm-cancel", policy)
	cancel()

	done := make(chan struct{})
	go func() { r.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait() did not return within 1s after ctx cancel")
	}
}

func TestReconciler_DroppedSignalsDontPanic(t *testing.T) {
	acts := &fakeActions{startWait: time.Second} // wedge the goroutine
	r := New(acts, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	policy := &RespawnPolicy{Enabled: true, MaxRestarts: 100, WindowMs: 60000}
	r.Watch(ctx, "vm-busy", policy)
	// Flood the channel ; buffered=16, then drops kick in. No panic.
	var sent int64
	for i := 0; i < 200; i++ {
		r.Send(Signal{VMName: "vm-busy", Kind: SignalDown, When: time.Now()})
		atomic.AddInt64(&sent, 1)
	}
	if got := atomic.LoadInt64(&sent); got != 200 {
		t.Errorf("only sent %d ; loop blocked", got)
	}
}

// TestReconciler_HistoryWatchReplaceRace replays the cascade where
// the operator updates the SchedulingRule mid-flight : Unwatch(vm)
// then Watch(vm) again within ~ms. The OLD goroutine may still be
// inside handle()→History.Append while the NEW goroutine starts
// servicing fresh signals. Pre-fix History had no mu, so two
// goroutines could race on entries/append.
func TestReconciler_HistoryWatchReplaceRace(t *testing.T) {
	acts := &fakeActions{startWait: 5 * time.Millisecond}
	r := New(acts, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	policy := &RespawnPolicy{Enabled: true, MaxRestarts: 100, WindowMs: 60000}
	r.Watch(ctx, "vm-replay", policy)
	// 50 cascade iterations : send a signal, then Unwatch+Watch in
	// quick succession so the old goroutine's handle() likely
	// overlaps with the new goroutine's first signal.
	for i := 0; i < 50; i++ {
		r.Send(Signal{VMName: "vm-replay", Kind: SignalDown, When: time.Now()})
		r.Unwatch("vm-replay")
		r.Watch(ctx, "vm-replay", policy)
	}
	r.Send(Signal{VMName: "vm-replay", Kind: SignalDown, When: time.Now()})
	time.Sleep(50 * time.Millisecond)
}

// TestReconciler_SendUnwatchRace exercises the concurrent path that
// could panic pre-fix : if Send releases mu before doing the
// non-blocking channel write, Unwatch can close+delete the channel
// in the meantime → send-on-closed-channel panic. Now Send holds mu
// across the entire read+send, so close vs send are mutually
// exclusive. Run with `go test -race` for full coverage.
func TestReconciler_SendUnwatchRace(t *testing.T) {
	acts := &fakeActions{}
	r := New(acts, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	policy := &RespawnPolicy{Enabled: true, MaxRestarts: 100, WindowMs: 60000}

	// 20 distinct VM names ; the existing vmName(i) helper builds
	// from 'a'+i so we stay under its rune range. Distinct names so
	// the per-VM State (History) doesn't race separately ; what
	// matters here is Send + Unwatch on the SAME map slot completing
	// without send-on-closed-channel panics.
	const N = 20
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		vmName := vmName(i)
		r.Watch(ctx, vmName, policy)
		wg.Add(2)
		go func() {
			defer wg.Done()
			r.Send(Signal{VMName: vmName, Kind: SignalDown, When: time.Now()})
		}()
		go func() {
			defer wg.Done()
			r.Unwatch(vmName)
		}()
	}
	wg.Wait()
}
