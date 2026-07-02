# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Loom is a Go library for durable, event-sourced workflow execution with
first-class LLM steps (budget ledger, fallback chains, approval gates).
Embedded-first: SQLite state file, in-process executor, no CGO, no server.

## Commands

Two Go modules: the library at the root, and `examples/triage` (own go.mod
with a `replace ../..` — demo deps must not leak into the library's import
graph). Commands assume the repo root unless noted.

```sh
go build ./... && go vet ./...        # library
go test ./...                         # all tests incl. e2e (~5s)
go test ./e2e/ -v                     # subprocess crash tests only (unix; builds examples/triage)
go test ./ -run TestFoldRetry -v      # single test
cd examples/triage && go build ./...  # example module (build/vet/tidy separately)
```

The flagship demo doubles as a manual smoke test:

```sh
cd examples/triage && go build -o triage .
./triage -db demo.db -id t1 run -crash-at draft   # dies mid-LLM-step on purpose
./triage -db demo.db -id t1 run                   # resumes; prints recovery transcript
./triage -db demo.db -id t1 approve && ./triage -db demo.db -id t1 run
./triage -db demo.db inspect                      # embedded UI on :7710
```

## The governing document

**ARCHITECTURE.md is a decision register (ADR-1..18), not documentation.**
Every load-bearing choice has named alternatives and rejection reasons.
Standing rule from the project owner: if implementation reveals a
contradiction with any ADR, stop and surface it — never silently resolve
it. If forced to choose between cutting a feature and compromising a
registered decision, cut the feature and note it in the out-of-scope list.
When a resolved deviation is accepted, record it in the register as an
"implementation note (surfaced deviation)" — there are existing examples.
`docs/design-grilling.md` preserves the design-evolution history.

## Architecture (the parts that span multiple files)

**Everything is a fold over an append-only event log** (ADR-1). An
instance's state is computed by folding its events (`fold.go`); the fold
returns a `Prescription` — the single next action. `driver.run` in
`executor.go` loops: load, fold, execute prescription, append, repeat.
Crash recovery, normal execution, backpressure, and replay are all this
same loop; there is deliberately no recovery mode. Terminality is
fold-computed (`StepCompleted{Halt}` is the sole source; there is no
"workflow terminated" event — rule: derive, don't co-commit).

**Concurrency invariants live at the store's append, never in executor
logic** (ADR-7) — the append is the only serialization point that exists in
all deployment modes. Two append classes, both implemented in each store
(`store/memory`, `store/sqlite`):
- *Fenced*: executor-produced events require the claim epoch
  (`ClaimInstance` always grants and bumps it; stale appends get
  `ErrStaleEpoch` — steal-with-fencing, ADR-8).
- *Constrained*: recorded external inputs (`InstanceCreated`,
  `StepResumed`) are epoch-exempt, guarded by uniqueness (first-wins).

The `instances` table status column is a **derived cache** (the `Runnable`
index), never a fold input, and rebuildable. `Runnable` filters on it, so
cache-vs-log divergence in the unclaimable direction is a permanent wedge —
see the liveness note under ADR-7 and `status_liveness_test.go` before
touching any `SetStatus` call in the driver (the park-then-refold in the
`PrescribeAwaitResume` path exists to close a real race).

**The budget ledger is fused to the event log on purpose** (ADR-6):
reserve-then-settle events commit through the same append path as step
events. Reservations use the *sum* of per-hop chain maxes; crash-orphaned
reservations are settled at reserved max by the resuming executor. The
ledger may overcount after crashes, never undercount. Do not extract it
into a standalone package — the coupling is a registered decision.

**The factory convention is load-bearing** (ADR-13): workflows are defined
by factory functions taking their dependencies
(`NewTriageWorkflow(deps)`), because the replay harness (`replay/`) can
only mock side effects by re-Building the definition through the same
factory — only the LLM client is harness-injectable (`WithLLM`). Recorded
external inputs (approval decisions) replay from the log, never from
mocks.

**Definition compatibility** (ADR-5): instances carry creation-time suffix
hashes; on resume the next unexecuted step is anchored *by name* into the
current definition and the remaining suffix must match, else the instance
strands as `incompatible` (loudly, never guessing). This is why step names
must be unique per definition — validated at `Build`, like declared
`.Reads(...)`.

**Costs** are `loom.Cost` in nano-dollars (int64), not floats.

## Testing conventions

- The e2e tests (`e2e/`) kill the real demo binary with real signals
  (SIGKILL crash-resume; SIGSTOP/SIGCONT zombie fencing asserting exit
  code 3) — per the register, in-process crash simulation is not an
  acceptable substitute for these two.
- Store-behavior tests use wrapper types embedding `loom.StateStore` to
  inject torn writes or races deterministically (`status_liveness_test.go`).
- Everything except `e2e/` runs against the in-memory store with no
  external services.

## Commit style

No Co-Authored-By trailers (owner preference; prior history was rewritten
to strip them). Commit incrementally with messages that reference the ADRs
they implement or amend.
