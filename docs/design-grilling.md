# Design grilling — condensed transcript

Four rounds of adversarial design review, run before any code was written.
This is a condensed record: each challenge is recorded as *entering
position → challenge → exit position → where it landed in the register*.
The ADRs in ARCHITECTURE.md hold the full rationale; this file preserves the
design-evolution history — which positions were conceded, which were
defended, and why.

## Round one — the five questions the brief couldn't answer

**1. List or graph?** Entering: "find the first step without a persisted
result." Challenge: the flagship approval gate implies branching; loops
break step identity. Exit: MVP is explicitly linear; conditional
continuation becomes halt-with-status (terminal outcomes as data, not
control flow); step identity becomes `(stepName, attempt)`. → ADR-3, ADR-4.

**2. Log/code mismatch after a deploy?** Entering: undefined (versioning was
"out of scope"). Challenge: suspended approvals live for days, so
deploy-during-flight is the common case; the failure mode can't be cut with
the feature. Exit: definition-hash mismatch detection — fail loudly to an
`incompatible` state, never guess. Version *routing* stays future work;
*detection* does not. → ADR-5.

**3. Budget ledger vs at-least-once.** Entering: "checked transactionally
before each LLMStep." Challenge: the crash window (LLM call returned, result
not persisted) forces the ledger to lie in one direction — pick which. Exit:
reserve-then-settle, erring toward over-counting; no automatic refunds;
README asterisk: the ledger bounds recorded spend conservatively and may
exceed actual spend after crash-retries, never undercount it. → ADR-6.

**4. ApprovalStep breaks "log of step results."** Entering: principle 1 as a
missing-result scan. Challenge: a suspended instance has no result for the
approval step; suspension requires non-result entries. Exit: full
concession — the log is an event log with typed events, the executor is a
deterministic fold. Principle 1 rewritten. → ADR-1.

**5. Two executors, one instance.** Entering: no answer. Challenge: replicas
behind a load balancer, or a startup scanner racing a not-quite-dead
goroutine. Exit: documented single-writer for MVP, plus lease semantics
designed into the `StateStore` interface *now* — `ClaimInstance` with epoch
fencing; stale-epoch appends rejected. → ADR-8.

## Round two — grilling the answers

**1. Where does Halt live?** Forced from "terminal outcomes as data" to a
concrete engine-owned `StepOutcome{Halt, Status}` envelope; downstream
defensive-flag checking rejected (forgetting one silently sends a rejected
draft). → ADR-3.

**2. Full hash strands the safe.** Conceded: full-definition hash makes
every deploy strand pending approvals. Exit: suffix-only compatibility
(remaining steps), plus the stated limitation that "compatible" is
structural — step bodies are invisible. → ADR-5.

**3. Never-refund deadlocks.** Conceded: open-forever orphaned reservations
brick a crash-looping workflow pending manual action. Exit: settle
crash-orphaned reservations at reserved max, automatically, with an audit
event. Fallback-chain accounting introduced (initially: reserve at
max-across-models — corrected in round three). → ADR-6.

**4. Resume called twice with different answers.** Exit: first-wins,
enforced by a store uniqueness constraint at the append, not executor logic.
Generalized into the register's best sentence: every concurrency invariant
is enforced at the append, the only serialization point that exists in all
deployment modes. → ADR-7.

**5. Steal or lease?** Exit: steal-with-fencing; store guarantees safety,
caller owns liveness; leases rejected for smuggling clock assumptions into a
deployment-invariant contract. → ADR-8.

**6. What does replay actually assert?** Challenge: fold-replay
(Temporal-style) tests nothing when steps are arbitrary Go functions. Exit:
the harness is re-execution replay; determinism claim rescoped ("the fold is
deterministic; step re-execution is deterministic only with injectable
dependencies"); the constraint promoted into the step signature. → ADR-11.

**7. Ledger: library or feature?** Exit: coupled by design — a standalone
ledger that isn't transactional with step persistence reintroduces the
crash-window undercount. Coupling defended as the selling point; package
layout confesses it (no subtractable `loom/llm` leaf). → ADR-6, layout.

## Round three — the design contradicting itself

**1. Two sources of truth for "terminated."** Conceded: a crash between
`StepCompleted{Halt}` and an engine-appended `WorkflowTerminated` forces the
fold to derive terminality anyway. Exit: `StepCompleted{Halt}` is the sole
source; `WorkflowTerminated` deleted; rule coined: **derive, don't
co-commit**. → ADR-3.

**2. The suffix hash has no anchor.** Challenge: insert a step *before* the
current position — index-anchoring strands the unstrandable; name-anchoring
silently skips the inserted step for old instances. Exit: name-anchoring
(the instance's contract is its creation-time definition), which drags in
the step-name-uniqueness constraint, enforced at registration. → ADR-4,
ADR-5.

**3. Chain reservation undercounts — arithmetic, not philosophy.** Conceded
completely: failed hops bill input tokens and partial output, so worst case
is the **sum** of per-hop maxes, not the max. A 3-model chain is honestly a
3× worst-case liability, visible at configuration time. → ADR-6.

**4. Resume has no epoch.** Challenge: "every append carries the epoch"
contradicts a webhook-driven resume. Exit: the fenced/constrained append
taxonomy — epoch-fenced executor events vs uniqueness-constrained recorded
external inputs. Independently rediscovered the same category the replay
harness needs, taken as evidence the taxonomy is real. → ADR-7.

**5. The harness re-executes the approval step — who decides?** Exit: the
harness's second rule — side effects come from mocks; recorded external
inputs replay from the log; an unrecorded suspension stops the run
(`awaiting-unrecorded-input`, distinct from divergence). Post-divergence
comparisons marked advisory (trajectory noise). → ADR-11.

**6. Crash during a backoff wait.** Exit: the fold prescribes decisions with
not-before times computed from logged timestamps only; the executor owns the
clock — determinism preserved. Retry policy is engine-owned and declarative;
internal step retries forbidden (invisible to the log, break attempt
identity). Approval timeouts punted honestly: no durable timers in MVP,
caller-side sweep via `Resume(id, TimedOut)`, already race-safe via
first-wins. → ADR-9, ADR-10.

**7. The inspector replay button cannot do what its name says.** Conceded by
the design's own commitments (harness requires mocks; inspector holds real
deps). Exit: export-as-replay-test — teleport a production incident into a
failing local test. In-process re-execution rejected as dangerous;
fold-replay rejected as decorative. → ADR-12.

## Round four — the outside view

**1. The real competitor is the jobs table, not Temporal.** Exit: README
restructured — first screen speaks to the team about to hand-roll a
`pipeline_runs` table; platform comparisons demoted to a section for the
architect who scrolls. → ADR-18.

**2. The moat claim overreached.** Conceded: any durable-log engine could
ship a budget ledger. Exit: retreat to occupancy plus design quality —
embeddable-in-Go, CGO-free, zero-infra, LLM-aware is a niche the platforms
won't prioritize; competitor entry validates the problem. → ADR-18.

**3. The Go reviewer's first ten minutes.** Exit: `loom.Typed` generic
wrappers as the documented default (step bodies never see
`json.RawMessage`); declared `.Reads(...)` (amended post-review — closures
are opaque to registration); pure-Go SQLite (`modernc.org/sqlite`,
CGO_ENABLED=0 is the target user's literal deployment model); `ctx` as the
first parameter, never wrapped. → ADR-13, ADR-14.

**4. SIGTERM was undesigned.** Exit: four-phase shutdown — stop claiming,
grace, cancel step contexts, bounded append-drain — with the residual window
documented as reserved-max accounting working as designed. → ADR-15.

**5. Backpressure was undesigned.** Exit: the log is the queue —
`StartInstance` is one durable append; executors pull via the acquisition
loop up to `MaxConcurrentInstances`. Admission durability, backpressure, and
crash recovery are the same mechanism. → ADR-16.

**6. Retention was undesigned.** Exit: `store.Prune(olderThan)`, manual,
terminal-only, with the recorded tension: every pruned log is an incident
you can no longer export as a test — which is why a human, not a TTL,
decides. → ADR-17.

**7. Demo credibility.** Exit: the mock LLM bills realistic tokens (derived
from input length, real price table); the flagship demo kills itself
(`--crash-at`, hard exit in the worst window) and the recovery transcript is
the README's screen one. `LOOM_DEMO_LIVE` proves the dependency seam is
real, demoted to third. → layout, ADR-18.

## Post-review amendments

Six amendments were applied after the register was reviewed: (1)
`InstanceCreated` classified as a constrained append (ADR-7 had named
`StepResumed` as the class's only member — a contradiction with ADR-16's
unclaimed webhook-handler append); (2) ADR-13's registration-time `Output`
validation replaced with declared `.Reads(...)` — registration cannot see
inside closures; (3) the terminal-status set enumerated once in ADR-3
(`failed` had no named status and was silently unprunable); (4) the testing
strategy written down — subprocess SIGKILL crash test and SIGSTOP/SIGCONT
zombie-fencing test as the canonical end-to-end pair; (5) ADR-5's worked
example and the anchor-missing (renamed/deleted step) edge restored; (6)
`examples/triage` isolated behind its own go.mod, and this transcript
committed.
