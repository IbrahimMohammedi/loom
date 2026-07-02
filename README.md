# Loom

**Durable step orchestration for Go backends, with first-class LLM steps.
Embeddable: one import, one SQLite file, zero extra infrastructure.**

You're adding an AI feature to your Go service: classify → fetch context →
LLM call → human approval → send. You're about to build a `pipeline_runs`
table with a `current_step` column, a cron sweep, and a retry counter.

That table is a workflow engine with no attempt identity, no crash
accounting for the LLM call you already paid for, no answer for the
approval that waits three days across two deploys, and no way to turn
Friday's production incident into Monday's failing test.

Loom is that table, designed.

## Ten seconds of proof

The flagship demo kills itself mid-LLM-step — a hard `os.Exit(1)` after the
model call returned and before the result persisted, the worst possible
window:

```console
$ triage -db demo.db -id t1 run -crash-at draft
instance t1 created (budget $1.000000)
CRASH: exiting hard inside draft, after the LLM call, before the append

$ triage -db demo.db -id t1 run
instance t1 exists: resuming from its log (recovery is the same code path as normal execution)

  ✓ classify       recovered from log (not re-executed)
  ✓ fetch-context  recovered from log (not re-executed)
  $ draft          orphaned reservation settled at reserved max: $0.022750 (crash-orphaned)
  $ draft          settled at actual: $0.001137
  ✓ draft          completed
  ⏸ gate           suspended, awaiting approval

  status: suspended   recorded spend: $0.023887
  next:   triage -id t1 approve   (or reject)
```

Steps 1–2 were not re-run. The crashed attempt's budget reservation was
settled at its reserved maximum — conservatively, with an audit event —
and execution continued from where it died. There is no recovery mode:
resuming a crashed instance and running a healthy one are the same fold
over the same event log.

Try it: `cd examples/triage && go build && ./triage run -crash-at draft && ./triage run`

## What you write

```go
wf := loom.NewWorkflow("triage")

wf.Step("classify", loom.Typed(func(ctx context.Context, sctx *loom.StepContext, t Ticket) (Classification, error) {
    return Classification{Category: categorize(t.Body)}, nil
}))

wf.Step("fetch-context", loom.Typed(func(ctx context.Context, sctx *loom.StepContext, c Classification) (TicketContext, error) {
    return TicketContext{Category: c.Category, KB: kb.Lookup(c.Category)}, nil
}))

wf.LLMStep("draft", chain, loom.Typed(func(ctx context.Context, sctx *loom.StepContext, tc TicketContext) (Draft, error) {
    resp, err := sctx.LLM().Complete(ctx, buildPrompt(tc)) // budget-guarded, fallback chain
    if err != nil {
        return Draft{}, err
    }
    return Draft{Body: resp.Text}, nil
}))

wf.Approval("gate") // suspends; resumes via loom.Resume from your webhook

wf.Step("send", loom.Typed(func(ctx context.Context, sctx *loom.StepContext, d loom.ApprovalDecision) (string, error) {
    var draft Draft
    sctx.Output("draft", &draft) // declared below; typos fail at startup
    return "sent", mailer.Send(sctx.IdempotencyKey(), to, draft.Body)
})).Reads("draft")

def, err := wf.Build() // duplicate names, bad Reads: registration errors, not runtime surprises
```

Steps are ordinary Go functions with `ctx` first and typed inputs/outputs —
`loom.Typed` owns the marshaling; your code never touches `json.RawMessage`.

| | jobs table + cron | Loom | neither |
|---|---|---|---|
| resume after crash without re-running completed steps | you'll build it, badly | ✓ event-sourced log, one code path | |
| LLM spend accounting that survives the crash window | no one builds this | ✓ reserve-then-settle, never undercounts | |
| exactly-once side effects | | | **impossible from the client side** — see below |

## At-least-once, not exactly-once (read this before adopting)

This is the most important design decision in the project.

Loom guarantees a step's **result is durably persisted before the workflow
advances**. It does **not** guarantee the step's real-world side effect
(send the email, call the LLM) happened exactly once — no client-side
system can. The crash window is irreducible: the side effect completes, the
process dies before the result persists, and on resume the step runs again.

Loom's contract: steps execute **at least once**, and side-effect
idempotency is the *step's* responsibility. The engine gives you the tool —
`sctx.IdempotencyKey()`, stable across re-executions of the same step in
the same instance — and the demo's mock mailer shows the pattern: a re-sent
email with a key the provider has already seen is dropped.

Systems that claim exactly-once side effects are describing their happy
path. We'd rather document the window than pretend it away.

## The budget ledger (and its honest asterisk)

Every `LLMStep` runs under a workflow-scoped budget, checked
transactionally with the event log:

1. **Reserve** before the call, at the chain's worst case — the **sum** of
   per-hop maxes, because a failed hop still bills input tokens and partial
   output. A 3-model fallback chain is honestly a 3× worst-case liability,
   and the reservation makes that visible at configuration time.
2. **Settle** after the call, at the actual cost across every hop attempted.
3. **Crash-orphaned reservations** (call returned, process died before
   settle) are settled at *reserved max* by the resuming executor, with an
   auditable `crash-orphaned` event. Never silently, never refunded
   automatically.

The asterisk: *the ledger bounds recorded spend, and records conservatively
— it may exceed actual spend after crash-retries, never undercount it. No
LLM provider deduplicates on client idempotency keys, so exactly-once spend
accounting is impossible from the client side.* Crash-retries consume
budget at reserved-max per orphaned attempt; a workflow crash-looping
inside its most expensive step exhausts its budget conservatively and halts
with status `budget_exhausted` — which is the guard working, not a bug.

The ledger is deliberately **not** a standalone package: reservation,
settlement, and step events commit through one append path, because a
ledger that isn't transactional with step persistence reintroduces the
exact crash-window undercount it exists to prevent. The coupling is the
feature.

## Replay: production incidents become failing tests

Any instance's event log can be re-executed locally against current code —
no external services:

```go
h := replay.New(fixture.Events,
    replay.WithLLM(mockLLM),
    replay.TruncateAt("draft", 1),                    // simulate the crash here
    // replay.InjectFailure("send", errors.New("x")), // or inject at the dependency
)
res, _ := h.Run(def) // res.Diverged, res.DivergedAt, res.StoppedAt
```

Two rules make it work: **side effects come from mocks** (build the
definition with mock deps — the same injectable seam the demo runs on), and
**recorded external inputs replay from the log** (the approval decision a
human made in production feeds back in; nobody mocks the human). LLM step
outputs are compared structurally, not byte-wise — replay regression-tests
everything *around* the model.

Scoped honestly: the fold is deterministic; step re-execution is
deterministic only if your steps take side effects through injectable
interfaces. That constraint is why `StepContext` exists.

The embedded inspector (`triage inspect`, or mount `inspector.New` in your
service) shows every instance's timeline, ledger lines, and suspension
state — and its **export-as-replay-test** button downloads the event log as
a fixture plus a generated test skeleton. There is deliberately no
in-process "replay" button: that would re-send emails and re-spend tokens
from a UI click.

## Architecture in one paragraph

A workflow instance is an append-only log of typed events. The executor is
a deterministic fold: compute state from the log, take the single next
action that state prescribes, append the outcome, repeat. Everything else
falls out of that: crash recovery (the fold doesn't care why the process
restarted), backpressure (`StartInstance` is one durable append; executors
pull work up to `MaxConcurrentInstances` — the log *is* the queue), zombie
safety (claims carry epochs; a stale executor's appends are rejected at the
store), retry backoff that survives restarts (the fold computes not-before
times from logged timestamps; only the executor reads the clock), and
deploy-safety (each instance carries suffix hashes of its creation-time
definition; changing not-yet-run steps strands the instance as
`incompatible` — loudly — instead of guessing).

The full decision register — 18 ADRs with alternatives considered and why
they lost — is in [ARCHITECTURE.md](ARCHITECTURE.md). The design review
that produced it is in [docs/design-grilling.md](docs/design-grilling.md).

## Operational notes

- **Single writer per SQLite file.** Embedded mode is one process per state
  file. **Running replicas behind a load balancer against per-replica
  SQLite files splits your workflow state.** Multi-replica needs the
  Postgres backend (future work); the `StateStore` interface already
  carries the fencing contract it will need.
- **Deploys:** `Engine.Shutdown(ctx)` stops claiming, lets steps finish,
  then cancels and drains appends. A deploy that interrupts an in-flight
  LLM step settles that attempt at reserved max on resume — crash-window
  accounting working as designed.
- **Approval timeouts:** suspended instances wait indefinitely; MVP has no
  durable timers. Run your own sweep and call `Resume(id, TimedOut)` — it's
  race-safe against a simultaneous human decision, first append wins.
- **Retention:** logs live until you call `store.Prune(olderThan)` —
  manual, terminal-instances-only, because every pruned log is an incident
  you can no longer export as a replay test. LLM prompts/outputs are step
  results; know your bytes-per-instance.
- **CGO-free:** pure-Go SQLite (`modernc.org/sqlite`); `CGO_ENABLED=0`
  cross-compiles and scratch containers work.

## How this relates to Temporal, DBOS, Restate, LangGraph

Temporal/Conductor do durable orchestration at platform scale — with a
server, a database you operate, and no concept of an LLM call as a step
type. LangGraph/CrewAI have LLM-native primitives and no durability: crash
mid-run, pay for the tokens twice. DBOS and Restate are closer cousins —
durable-log engines — and could ship a budget ledger in a quarter; if they
do, that validates the problem. Loom's position isn't unreplicability, it's
occupancy: **embeddable-in-Go, CGO-free, zero-infra, LLM-aware** is a niche
the platforms won't prioritize, and inside it every decision is written
down with the alternatives it beat. Loom is for the team that wants durable
LLM steps without adopting a platform.

## Future work (deliberately not in MVP)

- Postgres + NATS JetStream distributed mode (the interfaces are already
  shaped for it: fenced appends, dispatcher-arbitrated claims)
- Real LLM provider SDKs (the `LLMClient` seam is proven by the demo's
  `LOOM_DEMO_LIVE=1` path, which drives a real model through the same
  interface as the mock)
- Version routing for in-flight instances (mismatch *detection* ships now)
- Durable timers / approval SLAs
- DAGs, loops, branching (MVP is linear + halt-with-status; the
  retry-with-revision loop is the forcing function for a real graph)
- Payload externalization for large prompts; deletion audit
- Inspector auth/multi-tenancy

## License & status

MVP. The core contract (event log, fold, `StateStore`) is designed to be
stable; surface APIs may move. Run the demo, kill it, and read
[ARCHITECTURE.md](ARCHITECTURE.md) before depending on it.
