<p align="center"><img src="https://raw.githubusercontent.com/go-proc/brand/main/social/go-proc-respawn.png" alt="go-proc/respawn" width="720"></p>

# respawn — go-proc

[![Go Reference](https://pkg.go.dev/badge/github.com/go-proc/respawn.svg)](https://pkg.go.dev/github.com/go-proc/respawn)
[![Docs](https://img.shields.io/badge/docs-mkdocs--material-B45309)](https://go-proc.github.io/docs/)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)
[![Go](https://img.shields.io/badge/go-1.26.4%2B-00ADD8)](https://go.dev/dl/)
[![Coverage](https://img.shields.io/badge/coverage-100%25-1a7f37)](#tests--coverage)

**A pure-Go (no cgo) restart / reconcile state machine.** Feed it a stream of
`down` / `up` / `unhealthy` / `healthy` signals for named units, and it decides
*when* to respawn after a death — honouring a grace period (anti-flap),
`max_restarts` within a sliding window (anti-thrash), and optional exponential
backoff — then effects the respawn through a caller-supplied `Actions`
interface.

Extracted from a microVM agent and made **fully dependency-free**: the
`RespawnPolicy` is a local value type (no external schema), health is a local
`SignalKind` (no probe/health-checker import), and side effects go through an
interface, so the whole machine is unit-testable with a fixed clock and a fake.

> **A pure decision core.** `Decide` is an allocation-light pure function:
> `(policy, history, attempt, signal, now) → Plan`. The `Reconciler` is the
> concurrent shell around it — one goroutine per watched unit, lazily started,
> draining a buffered signal channel — but every scheduling decision is a value
> you can assert against a fixed clock.

## Features

- **Grace period** — a `down`/`unhealthy` signal parks in `grace_wait` for
  `grace_period_ms` first, so a transient flap that recovers never triggers a
  restart.
- **Anti-thrash window** — `max_restarts` restarts are allowed per rolling
  `window_ms`; exceeding it moves the unit to `cooldown` until the window rolls.
- **Backoff** — `constant` or `exponential` (doubling `initial_delay_ms`, capped
  at 5 minutes) before each respawn.
- **Unhealthy = stop-then-start** — a liveness failure on an otherwise-running
  unit is recovered by `Stop` then `Start`, not a bare `Start`.
- **Concurrent, per-unit** — one `Reconciler` fans out across many units by
  name; distinct units never serialise on each other.

CGO-free, dependency-free, **100% test coverage** (including every error
branch), race-tested, `gofmt` + `go vet` clean, and green across the six 64-bit
Go targets (amd64, arm64, riscv64, loong64, ppc64le, s390x).

## Install

```sh
go get github.com/go-proc/respawn
```

## Usage

```go
package main

import (
	"context"
	"time"

	"github.com/go-proc/respawn"
)

// myActions effects the respawn. A real one wraps your process/VM driver;
// tests supply a fake to assert state-machine behaviour.
type myActions struct{ /* ... */ }

func (myActions) StartVM(ctx context.Context, name string) error { /* ... */ return nil }
func (myActions) StopVM(ctx context.Context, name string) error  { /* ... */ return nil }

func main() {
	r := respawn.New(myActions{}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	policy := &respawn.RespawnPolicy{
		Enabled:        true,
		GracePeriodMs:  2000,          // debounce flaps
		MaxRestarts:    5,             // within...
		WindowMs:       60000,         // ...a 60s window
		Backoff:        "exponential", // 1s, 2s, 4s, ... capped 5m
		InitialDelayMs: 1000,
	}
	r.Watch(ctx, "web-1", policy)

	// Feed signals from your event source:
	r.Send(respawn.Signal{VMName: "web-1", Kind: respawn.SignalDown, When: time.Now()})

	// On shutdown: cancel ctx, then drain.
	cancel()
	r.Wait()
}
```

Or drive the pure core directly, no goroutines:

```go
plan := respawn.Decide(policy, history, attempt, respawn.Signal{Kind: respawn.SignalDown}, time.Now())
// plan.State / plan.Action / plan.DelayFor / plan.Reason
```

## API

```go
type RespawnPolicy struct {
	Enabled        bool
	GracePeriodMs  int64
	MaxRestarts    int32
	WindowMs       int64
	Backoff        string // "constant" | "exponential"
	InitialDelayMs int64
}

type SignalKind int // SignalDown | SignalUp | SignalUnhealthy | SignalHealthy
type Signal struct { VMName string; Kind SignalKind; When time.Time }

// Actions is the side-effect surface the reconciler drives.
type VMActions interface {
	StartVM(ctx context.Context, name string) error
	StopVM(ctx context.Context, name string) error
}

// Reconciler — the concurrent shell.
func New(actions VMActions, log *slog.Logger) *Reconciler
func (r *Reconciler) Watch(ctx context.Context, name string, policy *RespawnPolicy)
func (r *Reconciler) Unwatch(name string)
func (r *Reconciler) Send(sig Signal)
func (r *Reconciler) Wait()

// Pure decision core.
type Plan struct { State State; Action Action; DelayFor time.Duration; Reason string }
func Decide(policy *RespawnPolicy, h *History, attempt int, sig Signal, now time.Time) Plan
func DecideAfterGrace(policy *RespawnPolicy, h *History, attempt int, kind SignalKind, now time.Time) Plan
```

> **Naming.** The `Actions` methods and `Signal.VMName` keep the `VM` names of
> the origin codebase; the package itself is unit-agnostic — a "VM" here is any
> named thing you supervise.

## Tests & coverage

The pure `Decide` core is asserted against a fixed clock across every policy
branch; the concurrent `Reconciler` is driven through a fake `Actions` with
injected clock/sleep seams to cover backoff, cooldown, grace, and every error
path deterministically.

```sh
COVERPKG=$(go list ./... | paste -sd, -)
go test -race -coverpkg="$COVERPKG" -coverprofile=cover.out ./...
go tool cover -func=cover.out | tail -1   # 100.0%
```

## License

BSD-3-Clause — see [LICENSE](LICENSE). Copyright the go-proc/respawn authors.
