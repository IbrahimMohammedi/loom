package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

type Config struct {
	Store StateStore
	// LLM is the default client behind every LLMStep's chain caller.
	LLM        LLMClient
	ExecutorID string
	// MaxConcurrentInstances bounds the executor pool (ADR-16). A full pool
	// claims nothing; work waits durably in the store.
	MaxConcurrentInstances int
	// Tick is the acquisition-loop interval; dispatch latency is bounded by
	// it (ADR-16). Default 1s.
	Tick   time.Duration
	Logger *slog.Logger
}

type Engine struct {
	store StateStore
	llm   LLMClient
	id    string
	max   int
	tick  time.Duration
	log   *slog.Logger
	defs  map[string]*Definition

	mu     sync.Mutex
	active map[string]bool

	loopStop chan struct{}
	loopDone chan struct{}
	wg       sync.WaitGroup

	stepCtx    context.Context
	cancelStep context.CancelFunc
}

func NewEngine(cfg Config) (*Engine, error) {
	if cfg.Store == nil {
		return nil, errors.New("loom: Config.Store is required")
	}
	if cfg.MaxConcurrentInstances <= 0 {
		cfg.MaxConcurrentInstances = 16
	}
	if cfg.Tick <= 0 {
		cfg.Tick = time.Second
	}
	if cfg.ExecutorID == "" {
		host, _ := os.Hostname()
		if host == "" {
			host = "exec"
		}
		cfg.ExecutorID = fmt.Sprintf("%s-pid%d", host, os.Getpid())
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	sctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		store: cfg.Store, llm: cfg.LLM, id: cfg.ExecutorID,
		max: cfg.MaxConcurrentInstances, tick: cfg.Tick, log: cfg.Logger,
		defs: map[string]*Definition{}, active: map[string]bool{},
		stepCtx: sctx, cancelStep: cancel,
	}, nil
}

func (e *Engine) Register(def *Definition) { e.defs[def.name] = def }

func (e *Engine) Store() StateStore { return e.store }

// StartInstance is one durable append: accepted means persisted, nothing
// more (ADR-16). Execution happens when an executor pulls the instance.
func (e *Engine) StartInstance(ctx context.Context, workflow, id string, input any, budget Cost) error {
	def, ok := e.defs[workflow]
	if !ok {
		return fmt.Errorf("loom: workflow %q not registered", workflow)
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return err
	}
	ev := newEvent(EvInstanceCreated, "", 0, InstanceCreatedPayload{
		Workflow: workflow, Input: raw,
		StepNames: def.stepNames, SuffixHashes: def.suffixHashes,
		Budget: budget,
	})
	ev.Seq = 1
	return e.store.CreateInstance(ctx, id, workflow, ev)
}

// Resume records an external decision for a suspended instance (ADR-10).
func (e *Engine) Resume(ctx context.Context, id string, decision any) (ResumeResult, error) {
	return Resume(ctx, e.store, id, decision)
}

// Start launches the work-acquisition loop (ADR-16): the generalized
// startup scanner. Recovery of in-flight instances after a restart is not a
// special path — they are simply runnable work the loop picks up.
func (e *Engine) Start() {
	e.loopStop = make(chan struct{})
	e.loopDone = make(chan struct{})
	go func() {
		defer close(e.loopDone)
		t := time.NewTicker(e.tick)
		defer t.Stop()
		for {
			e.acquire()
			select {
			case <-e.loopStop:
				return
			case <-t.C:
			}
		}
	}()
}

func (e *Engine) acquire() {
	e.mu.Lock()
	slots := e.max - len(e.active)
	e.mu.Unlock()
	if slots <= 0 {
		return
	}
	ids, err := e.store.Runnable(context.Background(), time.Now(), slots*2)
	if err != nil {
		e.log.Error("loom: runnable query failed", "err", err)
		return
	}
	for _, id := range ids {
		e.mu.Lock()
		if e.active[id] || len(e.active) >= e.max {
			e.mu.Unlock()
			continue
		}
		e.active[id] = true
		e.mu.Unlock()
		e.wg.Add(1)
		go func(id string) {
			defer e.wg.Done()
			defer func() {
				e.mu.Lock()
				delete(e.active, id)
				e.mu.Unlock()
			}()
			if err := e.RunInstance(id); err != nil && !errors.Is(err, context.Canceled) {
				e.log.Error("loom: instance run ended with error", "instance", id, "err", err)
			}
		}(id)
	}
}

// RunInstance drives one instance until it is terminal, suspended, waiting
// on backoff, or incompatible. Exposed for tests and single-shot callers;
// normal operation goes through Start's acquisition loop.
func (e *Engine) RunInstance(id string) error {
	d := &driver{
		store: e.store, defs: e.defs, llm: e.llm, executorID: e.id,
		log: e.log, stepCtx: e.stepCtx,
	}
	// Store operations use a background context so the phase-4 drain can
	// persist results after step contexts are canceled (ADR-15).
	return d.run(context.Background(), id)
}

// Shutdown drains the engine in four phases (ADR-15): stop claiming, grace,
// cancel in-flight step contexts, bounded drain for appends. The residual
// window — a step canceled mid-LLM-call has nothing to append — is settled
// at reserved max on resume: crash-window accounting working as designed.
func (e *Engine) Shutdown(ctx context.Context) error {
	if e.loopStop != nil {
		select {
		case <-e.loopStop:
		default:
			close(e.loopStop)
		}
		<-e.loopDone
	}
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
	}
	e.cancelStep()
	select {
	case <-done:
		return nil
	case <-time.After(2 * time.Second):
		return errors.New("loom: shutdown drain window expired with steps still in flight")
	}
}

// DriveHooks intercepts execution for the replay harness (ADR-11). Nil
// fields are inert.
type DriveHooks struct {
	// BeforeStep may return an error to inject a failure at the step
	// boundary before the attempt starts.
	BeforeStep func(step string, attempt int) error
	// ResumeDecision supplies a recorded (or scripted) external input when
	// the fold prescribes AwaitResume. Without one, the driver leaves the
	// instance suspended.
	ResumeDecision func(step string) (json.RawMessage, bool)
	// Now overrides the executor's clock for NotBefore comparisons. The
	// fold itself never reads a clock (ADR-9).
	Now func() time.Time
}

// DriveOptions configures a single synchronous drive of one instance —
// the executor's inner loop, exposed for the replay harness.
type DriveOptions struct {
	Store       StateStore
	Definitions map[string]*Definition
	LLM         LLMClient
	ExecutorID  string
	Logger      *slog.Logger
	// StepContext is the cancellation scope handed to step bodies; store
	// operations use the ctx passed to Drive.
	StepContext context.Context
	Hooks       *DriveHooks
}

// Drive claims the instance and executes fold prescriptions until nothing
// is immediately runnable. Recovery and normal execution are this same
// loop (ADR-1).
func Drive(ctx context.Context, opts DriveOptions, instanceID string) error {
	d := &driver{
		store: opts.Store, defs: opts.Definitions, llm: opts.LLM,
		executorID: opts.ExecutorID, log: opts.Logger,
		stepCtx: opts.StepContext, hooks: opts.Hooks,
	}
	return d.run(ctx, instanceID)
}

type driver struct {
	store      StateStore
	defs       map[string]*Definition
	llm        LLMClient
	executorID string
	log        *slog.Logger
	stepCtx    context.Context
	hooks      *DriveHooks
}

func (d *driver) run(ctx context.Context, id string) error {
	if d.log == nil {
		d.log = slog.Default()
	}
	if d.hooks == nil {
		d.hooks = &DriveHooks{}
	}
	if d.stepCtx == nil {
		d.stepCtx = ctx
	}
	now := time.Now
	if d.hooks.Now != nil {
		now = d.hooks.Now
	}
	epoch, err := d.store.ClaimInstance(ctx, id, d.executorID)
	if err != nil {
		return err
	}
	parkedAt := ""
	for {
		events, err := d.store.Load(ctx, id)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return fmt.Errorf("loom: instance %s has an empty log", id)
		}
		var created InstanceCreatedPayload
		if err := json.Unmarshal(orNull(events[0].Payload), &created); err != nil {
			return err
		}
		def := d.defs[created.Workflow]
		if def == nil {
			_ = d.store.SetStatus(ctx, id, StatusIncompatible, nil)
			return fmt.Errorf("loom: workflow %q not registered for instance %s", created.Workflow, id)
		}
		st, p, err := Fold(def, events)
		if err != nil {
			return err
		}
		switch p.Kind {
		case PrescribeDone:
			return d.store.SetStatus(ctx, id, p.Status, nil)

		case PrescribeIncompatible:
			d.log.Warn("loom: instance incompatible with registered definition",
				"instance", id, "step", p.Step, "reason", p.Reason)
			return d.store.SetStatus(ctx, id, StatusIncompatible, nil)

		case PrescribeAwaitResume:
			if d.hooks.ResumeDecision != nil {
				if dec, ok := d.hooks.ResumeDecision(p.Step); ok {
					ev := newEvent(EvStepResumed, p.Step, p.Attempt, StepResumedPayload{Decision: dec})
					if _, err := d.store.AppendResumed(ctx, id, ev, &MetaUpdate{Status: StatusRunning}); err != nil {
						return d.yieldOnStale(ctx, id, err)
					}
					continue
				}
			}
			if parkedAt == p.Step {
				return nil
			}
			// Park, then re-fold once: a Resume racing this SetStatus
			// (webhook goroutine, no claim) applied status=running
			// atomically with its append; writing suspended over it would
			// wedge the instance — the cache says unclaimable, the log
			// holds a resume, and a retried Resume is first-wins and does
			// not re-apply meta. Re-folding sees any resume that landed
			// before our write and continues the drive; one that lands
			// after our write wins the cache and the acquisition loop
			// picks it up. Every interleaving is live.
			//
			// This is an instance of the ADR-7 re-fold invariant: any
			// driver write moving status toward unclaimable must be
			// followed by one re-fold, because constrained appends are
			// unfenced and can land inside the write. New parking step
			// types must do the same.
			if err := d.store.SetStatus(ctx, id, StatusSuspended, nil); err != nil {
				return err
			}
			parkedAt = p.Step
			continue

		case PrescribeFinalizeResume:
			sd := def.steps[def.index[p.Step]]
			fin := sd.finalize
			if fin == nil {
				fin = defaultFinalize
			}
			outcome, ferr := fin(p.Decision)
			if ferr != nil {
				if err := d.appendFailure(ctx, id, epoch, p.Step, p.Attempt, ferr, ""); err != nil {
					return d.yieldOnStale(ctx, id, err)
				}
				continue
			}
			if err := d.append(ctx, id, epoch, newEvent(EvStepCompleted, p.Step, p.Attempt,
				StepCompletedPayload{Output: outcome.Output, Halt: outcome.Halt, Status: outcome.Status}), nil); err != nil {
				return d.yieldOnStale(ctx, id, err)
			}
			continue

		case PrescribeRunStep:
			if !p.NotBefore.IsZero() && p.NotBefore.After(now()) {
				nb := p.NotBefore
				return d.store.SetStatus(ctx, id, StatusWaitingRetry, &nb)
			}
			if err := d.runStep(ctx, id, epoch, def, created, st, p); err != nil {
				return d.yieldOnStale(ctx, id, err)
			}
			continue

		default:
			return fmt.Errorf("loom: unknown prescription kind %d", p.Kind)
		}
	}
}

func (d *driver) runStep(ctx context.Context, id string, epoch int64, def *Definition, created InstanceCreatedPayload, st *FoldState, p Prescription) error {
	sd := def.steps[def.index[p.Step]]

	// Settle crash-orphaned reservations at reserved max (ADR-6): the lost
	// attempt is charged at the worst case it could have cost. Auditable,
	// never silent, never refunded automatically.
	for _, o := range p.Orphans {
		if err := d.append(ctx, id, epoch, newEvent(EvBudgetSettled, o.Step, o.Attempt,
			BudgetSettledPayload{Amount: o.Amount, Reason: "crash-orphaned"}), nil); err != nil {
			return err
		}
		d.log.Info("loom: settled crash-orphaned reservation at reserved max",
			"instance", id, "step", o.Step, "attempt", o.Attempt, "amount", o.Amount.String())
	}

	if sd.kind == kindApproval {
		return d.append(ctx, id, epoch, newEvent(EvStepSuspended, p.Step, p.Attempt, nil),
			&MetaUpdate{Status: StatusSuspended})
	}

	if d.hooks.BeforeStep != nil {
		if ierr := d.hooks.BeforeStep(p.Step, p.Attempt); ierr != nil {
			if err := d.append(ctx, id, epoch, newEvent(EvStepStarted, p.Step, p.Attempt, nil), nil); err != nil {
				return err
			}
			return d.appendFailure(ctx, id, epoch, p.Step, p.Attempt, ierr, "")
		}
	}

	if err := d.append(ctx, id, epoch, newEvent(EvStepStarted, p.Step, p.Attempt, nil),
		&MetaUpdate{Status: StatusRunning}); err != nil {
		return err
	}

	sctx := &StepContext{
		instanceID: id, step: sd, attempt: p.Attempt,
		workflowInput: created.Input, outputs: st.Outputs(),
	}

	var caller *ChainCaller
	if sd.kind == kindLLM {
		reserve := chainMaxCost(sd.chain)
		if created.Budget > 0 && st.Committed+reserve > created.Budget {
			err := fmt.Errorf("budget exhausted: committed %s + chain reservation %s exceeds budget %s",
				st.Committed, reserve, created.Budget)
			return d.appendFailure(ctx, id, epoch, p.Step, p.Attempt, Permanent(err), StatusBudgetExhausted)
		}
		if err := d.append(ctx, id, epoch, newEvent(EvBudgetReserved, p.Step, p.Attempt,
			BudgetReservedPayload{Amount: reserve, Reason: "chain max (sum of per-hop maxes)"}), nil); err != nil {
			return err
		}
		caller = &ChainCaller{client: d.llm, chain: sd.chain}
		sctx.llm = caller
	}

	outcome, serr := sd.fn(d.stepCtx, sctx, p.Input)

	// Settle actuals first, even for failed or canceled steps: the money is
	// spent whether or not the step succeeded. This append runs under the
	// store context, not the step context — the phase-4 drain (ADR-15).
	if caller != nil {
		if err := d.append(ctx, id, epoch, newEvent(EvBudgetSettled, p.Step, p.Attempt,
			BudgetSettledPayload{Amount: caller.Spent(), Reason: "actual"}), nil); err != nil {
			return err
		}
	}
	if serr != nil {
		// A cancellation from shutdown is a crash-equivalent: leave the
		// attempt unconcluded rather than recording a failure that would
		// count against retry policy (ADR-15).
		if errors.Is(serr, context.Canceled) || (errors.Is(serr, context.DeadlineExceeded) && d.stepCtx.Err() != nil) {
			return context.Canceled
		}
		return d.appendFailure(ctx, id, epoch, p.Step, p.Attempt, serr, "")
	}
	return d.append(ctx, id, epoch, newEvent(EvStepCompleted, p.Step, p.Attempt,
		StepCompletedPayload{Output: outcome.Output, Halt: outcome.Halt, Status: outcome.Status}), nil)
}

func (d *driver) append(ctx context.Context, id string, epoch int64, ev Event, meta *MetaUpdate) error {
	return d.store.Append(ctx, id, epoch, ev, meta)
}

func (d *driver) appendFailure(ctx context.Context, id string, epoch int64, step string, attempt int, serr error, reason string) error {
	p := StepFailedPayload{Error: serr.Error(), Permanent: IsPermanent(serr), Reason: reason}
	return d.append(ctx, id, epoch, newEvent(EvStepFailed, step, attempt, p), nil)
}

// yieldOnStale converts a stale-epoch rejection into a logged yield: a
// newer claimant owns the instance now; this executor's view is dead
// (ADR-8). Any other error passes through.
func (d *driver) yieldOnStale(_ context.Context, id string, err error) error {
	if errors.Is(err, ErrStaleEpoch) {
		d.log.Warn("loom: append rejected (stale epoch): yielding instance to newer claimant", "instance", id)
	}
	return err
}
