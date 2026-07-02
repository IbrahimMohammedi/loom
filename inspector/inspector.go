// Package inspector is Loom's embedded HTTP UI (ADR-12): instance list,
// per-step timeline over the fold, status surfacing for the states the
// design manufactures (suspended, incompatible with both hashes,
// budget_exhausted, terminal-with-status), manual resume for suspended
// instances (the same constrained-append path as a webhook), and
// export-as-replay-test — there is deliberately no in-process replay
// button: re-execution with real deps would re-send emails and re-spend
// tokens from a UI click, and fold-replay tests nothing users care about.
// The export's job: teleport a production incident into a failing local
// test.
//
// MVP scope note: no auth or multi-tenancy — mount it on an internal
// listener.
package inspector

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"strings"
	"time"

	"github.com/IbrahimMohammedi/loom"
	"github.com/IbrahimMohammedi/loom/replay"
)

type Inspector struct {
	store loom.StateStore
	defs  map[string]*loom.Definition
	mux   *http.ServeMux
}

// New builds the inspector handler. Mount it wherever the host service
// serves internal traffic: http.Handle("/loom/", http.StripPrefix("/loom", insp)).
func New(store loom.StateStore, defs map[string]*loom.Definition) *Inspector {
	i := &Inspector{store: store, defs: defs, mux: http.NewServeMux()}
	i.mux.HandleFunc("GET /{$}", i.list)
	i.mux.HandleFunc("GET /instance/{id}", i.detail)
	i.mux.HandleFunc("POST /instance/{id}/resume", i.resume)
	i.mux.HandleFunc("GET /instance/{id}/fixture.json", i.fixture)
	i.mux.HandleFunc("GET /instance/{id}/replay_test.go", i.testSkeleton)
	return i
}

func (i *Inspector) ServeHTTP(w http.ResponseWriter, r *http.Request) { i.mux.ServeHTTP(w, r) }

func (i *Inspector) list(w http.ResponseWriter, r *http.Request) {
	metas, err := i.store.List(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	render(w, listTmpl, metas)
}

type stepRow struct {
	Name       string
	Kind       string
	Status     string
	Attempts   int
	NotBefore  string
	Spend      string
	SpendNotes []string
	At         string
}

type detailView struct {
	Meta        loom.InstanceMeta
	Steps       []stepRow
	Events      []loom.Event
	Suspended   bool
	Incompat    bool
	IncompatMsg string
	TotalSpend  string
}

func (i *Inspector) detail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	meta, err := i.store.Meta(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	events, err := i.store.Load(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	v := detailView{Meta: meta, Events: events,
		Suspended: meta.Status == loom.StatusSuspended,
		Incompat:  meta.Status == loom.StatusIncompatible,
	}
	v.Steps, v.TotalSpend = i.timeline(events)
	if v.Incompat {
		v.IncompatMsg = i.incompatReason(events)
	}
	render(w, detailTmpl, v)
}

// timeline folds the event log into per-step rows: status, attempts,
// backoff countdowns, and the budget lines including crash-orphaned
// settlements (already settled at reserved max — displaying them is a
// bookkeeping audit trail, not a rescue operation).
func (i *Inspector) timeline(events []loom.Event) ([]stepRow, string) {
	if len(events) == 0 {
		return nil, ""
	}
	var created loom.InstanceCreatedPayload
	_ = json.Unmarshal(events[0].Payload, &created)

	rows := map[string]*stepRow{}
	order := created.StepNames
	for _, n := range order {
		rows[n] = &stepRow{Name: n, Status: "pending"}
	}
	def := i.defs[created.Workflow]
	if def != nil {
		for _, si := range def.Steps() {
			if r, ok := rows[si.Name]; ok {
				r.Kind = si.Kind
			}
		}
	}
	var total loom.Cost
	for _, ev := range events[1:] {
		row, ok := rows[ev.Step]
		if !ok {
			continue
		}
		row.At = ev.Time.Format(time.RFC3339)
		switch ev.Type {
		case loom.EvStepStarted:
			if ev.Attempt > row.Attempts {
				row.Attempts = ev.Attempt
			}
			if row.Status == "pending" || row.Status == "failed (retrying)" {
				row.Status = "running"
			}
		case loom.EvStepCompleted:
			var p loom.StepCompletedPayload
			_ = json.Unmarshal(ev.Payload, &p)
			if p.Halt {
				row.Status = "completed (halted: " + p.Status + ")"
			} else {
				row.Status = "completed"
			}
		case loom.EvStepFailed:
			var p loom.StepFailedPayload
			_ = json.Unmarshal(ev.Payload, &p)
			if p.Permanent {
				row.Status = "failed (permanent)"
				if p.Reason != "" {
					row.Status = "failed (" + p.Reason + ")"
				}
			} else {
				row.Status = "failed (retrying)"
			}
		case loom.EvStepSuspended:
			row.Status = "suspended (awaiting approval)"
			if ev.Attempt > row.Attempts {
				row.Attempts = ev.Attempt
			}
		case loom.EvStepResumed:
			row.Status = "resumed"
		case loom.EvBudgetReserved:
			var p loom.BudgetReservedPayload
			_ = json.Unmarshal(ev.Payload, &p)
			row.SpendNotes = append(row.SpendNotes, fmt.Sprintf("reserved %s (attempt %d)", p.Amount, ev.Attempt))
		case loom.EvBudgetSettled:
			var p loom.BudgetSettledPayload
			_ = json.Unmarshal(ev.Payload, &p)
			total += p.Amount
			row.Spend = p.Amount.String()
			row.SpendNotes = append(row.SpendNotes, fmt.Sprintf("settled %s (%s, attempt %d)", p.Amount, p.Reason, ev.Attempt))
		}
	}
	out := make([]stepRow, 0, len(order))
	for _, n := range order {
		out = append(out, *rows[n])
	}
	return out, total.String()
}

// incompatReason recomputes the fold against the registered definition to
// surface why the instance stranded — including both suffix hashes.
func (i *Inspector) incompatReason(events []loom.Event) string {
	if len(events) == 0 {
		return "empty log"
	}
	var created loom.InstanceCreatedPayload
	_ = json.Unmarshal(events[0].Payload, &created)
	def := i.defs[created.Workflow]
	if def == nil {
		return fmt.Sprintf("workflow %q is not registered in this process", created.Workflow)
	}
	_, p, err := loom.Fold(def, events)
	if err != nil {
		return err.Error()
	}
	if p.Kind == loom.PrescribeIncompatible {
		return p.Reason
	}
	return "instance is compatible with the currently registered definition; re-run it to clear this status"
}

func (i *Inspector) resume(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	approved := r.FormValue("approved") == "true"
	res, err := loom.Resume(r.Context(), i.store, id, loom.ApprovalDecision{Approved: approved, Note: "via inspector"})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if res.AlreadyResumed {
		// First-wins (ADR-10): tell the second approver what already won.
		w.WriteHeader(http.StatusConflict)
		fmt.Fprintf(w, "already decided: %s (first decision wins)", res.Decision)
		return
	}
	http.Redirect(w, r, "../"+id, http.StatusSeeOther)
}

func (i *Inspector) fixture(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	meta, err := i.store.Meta(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	events, err := i.store.Load(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s_fixture.json", id))
	_ = replay.WriteFixture(w, replay.Fixture{InstanceID: id, Workflow: meta.Workflow, Status: meta.Status, Events: events})
}

// testSkeleton generates the replay-test scaffold: fixture pre-wired,
// TruncateAt defaulted to the failed or suspended step, mock stubs for the
// LLM and TODOs for the definition's other dependencies.
func (i *Inspector) testSkeleton(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	meta, err := i.store.Meta(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	events, err := i.store.Load(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	target, attempt := incidentStep(events)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s_replay_test.go", sanitizeIdent(id)))
	fmt.Fprintf(w, testTemplate, meta.Workflow, id, sanitizeIdent(id), id, target, attempt)
}

// incidentStep picks the truncation default: the suspended or last-failed
// step, else the last step that ran.
func incidentStep(events []loom.Event) (string, int) {
	step, attempt := "", 1
	for _, ev := range events {
		switch ev.Type {
		case loom.EvStepFailed, loom.EvStepSuspended:
			step, attempt = ev.Step, max(ev.Attempt, 1)
		case loom.EvStepStarted:
			if step == "" {
				step, attempt = ev.Step, max(ev.Attempt, 1)
			}
		}
	}
	return step, attempt
}

func sanitizeIdent(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r == '-' || r == ' ' || r == '.' {
			b.WriteRune('_')
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func render(w http.ResponseWriter, t *template.Template, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := t.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// Serve is a convenience for the embedded default: an internal listener
// with no auth (out of MVP scope, stated in the package doc).
func Serve(ctx context.Context, addr string, store loom.StateStore, defs map[string]*loom.Definition) error {
	srv := &http.Server{Addr: addr, Handler: New(store, defs)}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	err := srv.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}
