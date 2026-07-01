# Loom — Architecture

Durable step orchestration for Go backends, with first-class primitives for
LLM-calling steps. Embeddable library, not a service. This document is the
decision register: every load-bearing choice, the alternatives considered,
and why the alternative lost. Decisions are numbered ADR-1..18 and
cross-referenced.

## Core principles (as they survived design review)

1. **A workflow instance is an append-only log of typed events.** The
   executor is a deterministic state machine: fold the event log to compute
   current state, then take the single next action that state prescribes.
   Recovery and normal execution are the same fold — there is no recovery
   mode. (This is a rewrite of the original "find first step without a
   result" formulation, which could not represent suspension. See ADR-1.)
2. **At-least-once step execution, never exactly-once.** The engine
   guarantees a step's result is durably persisted before the workflow
   advances. It does not guarantee the step's real-world side effect
   happened exactly once. Side-effect idempotency belongs to the step, via
   instanceID+stepName+attempt-derived keys.
3. **Embedded-first, distributed-optional.** Single Go binary, pure-Go
   SQLite, in-process executor pool, zero extra infra. Postgres/NATS is a
   future swap behind the same `StateStore` interfaces; workflow
   definitions and step code do not change between modes. Concurrency
   invariants are therefore enforced at the store's append (ADR-7), the
   only serialization point that exists in all modes.
4. **The budget ledger is fused to the durability layer, on purpose.**
   Reservation, settlement, and step events commit through one append
   path. A standalone ledger that is not transactional with step
   persistence reintroduces the crash-window undercount the ledger exists
   to prevent. Coupling is the selling point, not the embarrassment
   (ADR-6).
5. **Deterministic replay is the testing strategy — scoped honestly.** The
   fold is deterministic; step re-execution is deterministic only if steps
   take side effects through injectable dependencies. The replay harness is
   re-execution replay against mocks, and that dependency-injection
   constraint is visible in the step signature, not fine print (ADR-11,
   ADR-13).

---

## ADR-1: Event-sourced log with a fold-based executor

**Decision.** The instance state is an append-only sequence of typed events:
`InstanceCreated`, `StepStarted`, `StepCompleted`, `StepFailed`,
`StepSuspended`, `StepResumed`, `BudgetReserved`, `BudgetSettled`. Current
state is computed by folding the log. The fold's output is a prescription
(`RunStep`, `RetryStep{notBefore}`, `AwaitResume`, `Terminated{status}`);
the executor performs the prescription and appends the outcome.

**Alternatives.** (a) The original design: log of step *results*, executor
scans for the first step without one. Rejected because suspension
(`ApprovalStep`) has no result — representing it forces non-result entries
into a results log, at which point the log is an event log and the executor
is a state machine; pretending otherwise contorts the design. (b) Mutable
instance row with a `current_step` column. Rejected: loses attempt history,
replay, and audit; it is the hand-rolled jobs table this library replaces.

**Consequence.** Recovery, normal execution, backpressure (ADR-16), and
replay (ADR-11) are all consumers of the same fold.

## ADR-2: At-least-once, with idempotency as the step's job

**Decision.** Persist-before-advance is the engine's guarantee. Exactly-once
side effects are impossible from the client side (crash window between side
effect and append), so the engine does not pretend. Steps derive idempotency
keys from `(instanceID, stepName, attempt)` via a provided helper.

**Alternatives.** Exactly-once claims via transactional outboxes or
provider-side dedup. Rejected: LLM providers do not deduplicate on client
idempotency keys; an exactly-once claim would be a lie precisely where users
most need the truth. The README states this distinction front and center.

## ADR-3: Linear workflows; halt-with-status is the only control flow

**Decision.** MVP workflows are linear sequences. Conditional continuation
is expressed as terminal outcomes, not branches: every step returns

```go
type StepOutcome struct {
    Output json.RawMessage // step's data, opaque to engine
    Halt   bool            // workflow terminates after this step
    Status string          // terminal status when Halt (e.g. "rejected")
}
```

`StepCompleted{Halt: true}` is the **sole source of terminality**. There is
no separate `WorkflowTerminated` event; terminal status is fold-computed.

**Alternatives.** (a) DAGs/branching: rejected for MVP — "first unexecuted
step" and step identity both stop being well-defined; the retry-with-revision
loop is v1.1's forcing function to design a real graph. (b) Downstream steps
defensively checking an approval flag in data: rejected — forgetting the
check silently sends a rejected draft, the worst failure in the flagship
example. (c) An engine-appended `WorkflowTerminated` event after the halting
`StepCompleted`: rejected because a crash between the two appends forces the
fold to derive terminality from `Halt` anyway — two sources of truth, one
redundant. **Rule: derive, don't co-commit.** When one write must imply
another, make the second a derivation of the first rather than bolting
multi-event atomicity onto the store.

## ADR-4: Step identity is (stepName, attempt); names unique per definition

**Decision.** Every attempt of a step appends its own events; nothing is
overwritten. Step names are unique within a workflow definition, validated
at registration (registration fails otherwise).

**Why.** Attempt-scoped identity keeps the log append-only under retries and
makes per-attempt budget accounting (ADR-6) meaningful. Name uniqueness was
always latently required — resume idempotency (ADR-10) and suffix anchoring
(ADR-5) both assume it — so it is enforced loudly rather than assumed.

## ADR-5: Definition compatibility via name-anchored suffix hash

**Decision.** At instance creation, store a rolling hash chain over the
definition (hash of steps N..end, for each N; step names + step types only).
On resume, anchor by the **name** of the next unexecuted step, locate it in
the currently registered definition, and compare suffix hashes from there.
Mismatch → instance transitions to `incompatible`, execution refuses to
proceed, inspector shows both hashes. Never guess.

**Alternatives.** (a) No detection (original brief): rejected — suspended
approvals live for days, so deploy-during-flight is the common case, and
silent execution against a misaligned log is data corruption. (b) Full
definition hash: rejected — it strands instances on changes that provably
cannot affect them (edits to completed steps, appended steps), making every
deploy an incident. (c) Index-anchored suffix: rejected — a prefix-only
insertion strands the unstrandable. Name-anchoring means an old instance
never executes a step inserted before its position: the instance's contract
is the definition it was created under. If the operator's intent was "all
in-flight instances must run the new step," that is a data migration, not a
deploy, and no hash divines intent.

**Stated limitation.** "Compatible" means *structurally* compatible. Step
bodies — prompts, model choices, logic — are invisible to the hash;
behavioral drift between versions is by design not Loom's problem, because
hashing source would create false incompatibilities on refactors. Version
*routing* (running old instances against old code) is future work; version
*mismatch detection* is not punted.

## ADR-6: Budget ledger — reserve-then-settle, conservative by construction

**Decision.** Workflow-scoped ledger, transactional with the event log.
Before an `LLMStep` attempt: append `BudgetReserved` at the attempt's
configured max cost (guard trips if reservations + settlements would exceed
budget). After: append `BudgetSettled` at actual cost. Crash-orphaned
reservations are settled **at reserved max** by the resuming executor
(`BudgetSettled{amount: reservedMax, reason: "crash-orphaned"}` — auditable,
never silent, never refunded automatically). Fallback chains take **one
reservation sized at the sum of per-hop maxes**, settled once at the sum of
actuals across attempted hops.

**Alternatives.** (a) Charge-on-settle only: rejected — the crash window
(LLM call returned, append pending) makes the ledger undercount the thing it
exists to bound. (b) Leave orphaned reservations open forever: rejected —
repeated crashes accumulate phantom reservations until the workflow deadlocks
awaiting manual action; settling at max keeps the same conservatism without
an operator in the loop. (c) Chain reservation at max-across-models:
rejected on arithmetic — failed hops bill real money (input tokens always,
partial output often), so worst case is the sum, and a 3-model chain is
honestly a 3× worst-case liability, visible at configuration time. (d) A
standalone `budgetledger` package usable with any executor: rejected — more
reusable and strictly worse at its one job, because a ledger outside the
append path reintroduces the crash-window lie. **The coupling is the
feature.**

**Documented guarantee.** The ledger bounds recorded spend and records
conservatively: it may exceed actual spend after crash-retries, never
undercount it. Crash-retries consume budget at reserved-max per orphaned
attempt; a workflow crash-looping inside its most expensive step exhausts
its budget conservatively and halts with status `budget_exhausted` — which
is the guard working, not a bug.

## ADR-7: Append taxonomy — every event type carries an append-time guard

**Decision.** The append is the serialization point; every concurrency
invariant is enforced there, because it is the only serialization point that
exists in all deployment modes. Two classes:

- **Fenced appends** (claim epoch required): every event an executor
  produces while driving an instance — step events, reservations,
  settlements. Guard against zombie executors; stale-epoch appends rejected.
- **Constrained appends** (epoch-exempt, uniqueness-guarded): events
  recording **external inputs**. MVP's only member: `StepResumed{decision}`,
  unique on `(instanceID, stepName, attempt, eventType)`. Safe unfenced
  because the constraint makes it idempotent-first-wins and the fold only
  consumes it when suspended at that step.

**Why two classes.** A webhook-driven `Resume` has no claim; forcing it to
claim would fence off a legitimately running executor as a side effect of a
human click. The taxonomy also names "recorded external inputs" as a
distinct thing in the log — the same category the replay harness needs
(ADR-11), discovered independently from the concurrency side. Future
auto-firing approval timeouts would append as constrained too.

## ADR-8: Instance ownership — steal-with-fencing, no leases

**Decision.**

```go
// ClaimInstance grants ownership unconditionally and increments the instance
// epoch. Appends bearing a stale epoch are rejected (safety). The store does
// NOT arbitrate between live claimants; callers must ensure one claimant per
// instance (liveness). In embedded mode the process is the arbiter; in
// distributed mode the dispatcher is.
ClaimInstance(id InstanceID, executor ExecutorID) (Epoch, error)
```

**Alternatives.** Leases with TTL + heartbeats: rejected — they smuggle
clock-synchronization assumptions and a deployment-specific tuning parameter
into a contract that principle 3 requires to be deployment-invariant. The
nastiest race (startup scanner resumes an instance while a not-quite-dead
goroutine finishes a step) is closed by fencing: the scanner's claim bumps
the epoch; the zombie's append carries the old epoch; the store rejects it.

## ADR-9: Retries — engine-owned policy; the fold owns time, the executor owns the clock

**Decision.** Per-step declarative policy at definition time
(`loom.Retry{Max: 3, Backoff: loom.Exponential(30 * time.Second)}`). Steps
signal non-retryable failure by wrapping with `loom.Permanent(err)`; steps
never implement internal retry loops (an internal retry is invisible to the
log and breaks attempt identity and per-attempt reservation accounting).
The fold prescribes `RetryStep{step, attempt, notBefore: failedAt +
backoff(attempt)}` computed **only from logged timestamps and declared
policy** — the fold never reads the wall clock, so it stays deterministic.
The executor compares `notBefore` to its clock. A crash during backoff costs
nothing: the restarted fold prescribes the same `notBefore`.

**Noted.** `notBefore` is evaluated against the executing node's clock;
skew shifts retry timing by the skew amount — harmless for backoff, and
stated so nobody builds SLA logic on retry timestamps.

## ADR-10: ApprovalStep — suspend/resume, first-wins, no durable timers in MVP

**Decision.** `ApprovalStep` appends `StepSuspended`; the fold prescribes
`AwaitResume`. `Resume(instanceID, decision)` appends `StepResumed` as a
constrained append (ADR-7): the second delivery — webhook retry, racing
approver, timeout sweep — violates the uniqueness constraint, and `Resume`
returns the recorded decision with `AlreadyResumed = true`.

**Timeouts: punted, in the doc comment where users will read it.** Suspended
instances wait indefinitely. MVP has no durable timers — there is no
executor awake to fire one, and a timer poller has real semantics (who
fires, what epoch, what if it races a human) that deserve design, not a
bolt-on. Timeout policy is the caller's job: run a sweep and call
`Resume(id, TimedOut)` — which the constrained-append machinery already
makes safe against racing a simultaneous human decision: first append wins,
deterministically.

## ADR-11: Replay harness — re-execution replay, with two rules

**Decision.** The harness re-executes steps against the recorded log:

```go
harness := loom.NewReplayHarness(log,
    loom.WithDeps(mockLLM, mockMailer),        // side effects behind interfaces — required
    loom.TruncateAt("draft-reply", 1),         // or: loom.InjectFailure("send", ErrTimeout)
)
result := harness.Run(currentDefinition)
// result.Diverged, result.DivergedAt, result.Recorded, result.Replayed,
// result.StoppedAt (reason: awaiting-unrecorded-input), per-step Advisory flag
```

**Rule 1: side effects come from mocks; recorded external inputs replay from
the log.** Re-execution reaching a suspension consults the log for the
recorded `StepResumed` at that `(stepName, attempt)` and feeds it in — no
blocking, no mocking of humans. A suspension with no recorded resume stops
the run with `StoppedAt: awaiting-unrecorded-input` (not a divergence — the
code under test didn't fail; reality never supplied the input).
`WithResumeDecision(...)` lets a test script hypothetical inputs, making the
harness a what-if tool.

**Rule 2: the first divergence is the finding; everything after is
advisory.** After content diverges at step N, step N+1's replayed input
differs from its recorded input, so downstream comparisons compare
trajectories, not code. `DivergedAt` is authoritative; later comparisons are
marked `Advisory`.

**Two injection surfaces, deliberately distinct.** `InjectFailure` acts at
the dependency level (tests the step's error handling and engine
retry/halt); `TruncateAt` acts at the log level (simulates a crash, tests
resume-from-checkpoint).

**Rejected.** Fold-replay as the harness (Temporal-style): with steps as
arbitrary Go functions it tests the engine, not the user's code — nearly
nothing of user value. The LLMStep comparator defaults to structural
(shape/schema) comparison, not bytes: recorded LLM output never byte-matches
re-execution, and what replay regression-tests is everything *around* the
LLM call.

## ADR-12: Inspector — no in-process replay button; export instead

**Decision.** The embedded HTTP inspector ships: a timeline view over the
fold (per-step status, attempts, timestamps, `notBefore` countdowns); status
surfacing for the states this design manufactures (`suspended`,
`incompatible` with both hashes shown, `budget_exhausted`, terminal-with-
status); orphaned-reservation display with crash-orphaned audit events and a
release/bookkeeping action; manual `Resume` for suspended instances (same
constrained-append path as the webhook); and **export-as-replay-test** —
download the instance's event log as a fixture plus a generated Go test
skeleton (`NewReplayHarness` pre-wired, `TruncateAt` defaulted to the
failed/suspended step, mock stubs for the definition's declared
dependencies). The button's job: teleport a production incident into a
failing local test.

**Rejected.** (a) In-process re-execution with real deps: re-sends emails
and re-spends tokens from a UI click — a re-run wearing a replay costume.
(b) In-process fold-replay: safe but decorative (see ADR-11's rejection).
The harness's own requirement (mocked deps) makes export the only coherent
button.

## ADR-13: Typed steps; ctx first; references validated at registration

**Decision.** The documented default step shape:

```go
func DraftReply(ctx context.Context, sctx loom.StepContext, in TicketContext) (Draft, error)

wf.Step("draft-reply", loom.Typed(DraftReply))
```

`loom.Typed` owns marshaling both ways; step bodies never see
`json.RawMessage` even though the log stores it. Raw steps are the escape
hatch, not the default. `ctx context.Context` is the first parameter per Go
convention — `StepContext` is Loom's surface (deps, prior outputs, LLM
client, budget) and does **not** wrap ctx. Cross-step references
(`sctx.Output("classify", &v)`) are validated at definition registration —
a typo'd step name fails at startup, not in production.

**Stated limitation.** Output schema drift between steps remains a runtime
concern; Go generics cannot express heterogeneous pipeline typing without
code generation, which is out of MVP scope.

## ADR-14: Pure-Go SQLite (modernc.org/sqlite)

**Decision.** `modernc.org/sqlite`, no CGO. The performance penalty is
unmeasurable at 3–6 steps per workflow; the alternative (mattn) breaks
`CGO_ENABLED=0` cross-compiles and scratch containers — the literal
deployment model of the target user. A one-line go.mod decision that is
actually a pitch-integrity decision.

## ADR-15: Graceful shutdown — four phases, drain appends after canceling steps

**Decision.** `Engine.Shutdown(ctx)`: (1) stop claiming — the acquisition
loop exits; (2) grace — in-flight steps run to completion, appends flow;
(3) on ctx expiry, cancel in-flight step contexts; (4) drain — a short
bounded window (~2s) for canceled steps to return and completed-but-
unappended results to append before exit. Phase 4 shrinks the dangerous
window ("LLM call returned, append pending"): a step catching cancellation
after its call returned can still persist, converting a would-be
reserved-max burn into a clean completion.

**Documented residual.** A deploy that interrupts an in-flight LLM step
settles that attempt's reservation at reserved max on resume — the
crash-window accounting working as designed; the drain phase minimizes how
often you pay it. Steps must respect ctx (ADR-13's signature requires it).

## ADR-16: Backpressure — the log is the queue

**Decision.** `StartInstance` appends `InstanceCreated` and returns:
accepted means persisted, nothing more. Executors **pull**: a work-
acquisition loop (the generalized startup scanner) ticks (~1s default,
configurable), queries the store for prescribable work (unclaimed instances,
elapsed `notBefore` retries, resumed suspensions), and claims up to
`MaxConcurrentInstances` (required Engine config with a sane default). Pool
full → nothing claimed → work waits durably. A webhook flood is fast inserts
plus a pool grinding at its configured rate; nothing dropped, nothing
unbounded, handler latency is one append.

**Why.** Admission durability, backpressure, and crash recovery are the same
mechanism: the store is the only queue, and every consumer is the same
acquisition loop. Stated limitation: dispatch latency is bounded by the tick
interval — irrelevant for pipelines with human approval gates, noted for
anyone expecting sub-second dispatch.

## ADR-17: Retention — store.Prune(olderThan), manual, terminal-only

**Decision.** Eligible: terminal instances only (`completed`, `rejected`,
`budget_exhausted`) — never `suspended` or `incompatible`, which are waiting
for someone, not done. Explicit operator invocation; no background reaper.
`Prune` returns a count and appends nothing (the log does not log its own
deletions — a conscious cut; audit-of-deletion is a compliance feature, not
a durability feature).

**The recorded tension.** Every pruned log is a production incident that can
no longer be exported as a replay test — which is precisely why pruning is
manual: the operator, not a TTL, decides when history stops being evidence.
LLM payloads are step results and live until pruned; teams with large
prompts should know their bytes-per-instance. Payload externalization (blobs
outside the log, referenced by hash) is named future work.

## ADR-18: Positioning — the competitor is the jobs table, not Temporal

**Decision.** The README's first screen addresses the team about to build a
`pipeline_runs` table with a `current_step` column: that table is a workflow
engine with no attempt identity, no crash accounting for the LLM call
already paid for, no answer for the approval that waits three days across
two deploys, and no way to turn Friday's incident into Monday's failing
test. Loom is that table, designed. The Temporal/DBOS/Restate comparison
lives in a "How this relates to X" section below, in this register: the
defensible claim is **occupancy plus design quality** (embeddable-in-Go,
CGO-free, zero-infra, LLM-aware is a niche the platforms won't prioritize),
not unreplicability — a budget ledger is two event types and a fold rule,
and any durable-log engine could ship one. If they do, that validates the
problem: Loom exists for teams that want this without adopting a platform.

---

## Package layout

```
loom/                     core, one package by design (ADR-6: the ledger's
                          events are engine events; no loom/llm leaf package
                          pretending LLM-awareness is subtractable)
  events.go               event types, log encoding
  fold.go                 the fold: log -> state -> prescription
  executor.go             engine, acquisition loop, claims, shutdown phases
  workflow.go             definition builder, registration-time validation
                          (name uniqueness, output references), suffix hashes
  step.go                 Step, StepContext, StepOutcome, loom.Typed, Retry,
                          Permanent
  llmstep.go              LLMStep, fallback chain, LLMClient interface (mock-
                          able; real providers are future work)
  budget.go               ledger: reserve/settle events, guard, orphan
                          settlement
  approval.go             ApprovalStep, Resume, first-wins semantics
  idempotency.go          (instanceID, stepName, attempt) key helper
  store.go                StateStore interface: append (fenced/constrained),
                          ClaimInstance, queries, Prune

loom/store/sqlite/        pure-Go SQLite implementation (ADR-14)
loom/replay/              NewReplayHarness, injection, comparators, exporter-
                          consumed fixtures (ADR-11)
loom/inspector/           embedded HTTP UI: timeline, statuses, resume,
                          reservation release, export-as-replay-test (ADR-12)

examples/triage/          flagship demo: classify -> fetch-context -> LLM
                          draft (budget guard) -> approval gate -> send;
                          mock LLM bills realistic tokens (chars/4 + real
                          price table); --crash-at draft hard-exits (os.Exit)
                          after the mock call returns and before the append,
                          manufacturing the worst window; second invocation
                          prints steps 1-3 as recovered-from-log and the
                          orphaned reservation settled at max; optional
                          LOOM_DEMO_LIVE=1 wires a real provider behind the
                          same interface
```

## Explicitly out of scope for MVP (README lists as future work)

- Postgres + NATS JetStream distributed mode (interfaces designed for it:
  ADR-7, ADR-8)
- Real LLM provider SDKs (interface is pluggable; demo proves the seam)
- Version routing for in-flight instances (mismatch *detection* is in scope:
  ADR-5)
- Durable timers / approval timeouts (caller-side pattern documented: ADR-10)
- DAGs, loops, branching (linear + halt-with-status: ADR-3)
- Payload externalization, deletion audit (ADR-17)
- Inspector auth/multi-tenancy
