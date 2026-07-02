package inspector

import "html/template"

var listTmpl = template.Must(template.New("list").Parse(`<!doctype html>
<meta charset="utf-8"><title>Loom</title>
<style>` + css + `</style>
<h1>Loom instances</h1>
<table>
<tr><th>ID</th><th>Workflow</th><th>Status</th><th>Claimed by</th><th>Updated</th></tr>
{{range .}}<tr>
  <td><a href="instance/{{.ID}}">{{.ID}}</a></td>
  <td>{{.Workflow}}</td>
  <td><span class="st st-{{.Status}}">{{.Status}}</span></td>
  <td>{{.ClaimedBy}}</td>
  <td>{{.UpdatedAt.Format "2006-01-02 15:04:05"}}</td>
</tr>{{else}}<tr><td colspan=5>no instances</td></tr>{{end}}
</table>`))

var detailTmpl = template.Must(template.New("detail").Parse(`<!doctype html>
<meta charset="utf-8"><title>Loom — {{.Meta.ID}}</title>
<style>` + css + `</style>
<p><a href="../">&larr; instances</a></p>
<h1>{{.Meta.ID}} <span class="st st-{{.Meta.Status}}">{{.Meta.Status}}</span></h1>
<p>workflow <b>{{.Meta.Workflow}}</b> · epoch {{.Meta.Epoch}} · claimed by {{.Meta.ClaimedBy}} · recorded spend <b>{{.TotalSpend}}</b></p>

{{if .Incompat}}<div class="warn"><b>Incompatible:</b> {{.IncompatMsg}}<br>
Deploying a changed definition strands in-flight instances whose remaining
steps changed; drain or manually resolve. Version routing is future work.</div>{{end}}

{{if .Suspended}}<form method="post" action="{{.Meta.ID}}/resume" class="gate">
  awaiting approval:
  <button name="approved" value="true">approve</button>
  <button name="approved" value="false">reject</button>
  <span class="dim">(first decision wins — a racing webhook cannot double-decide)</span>
</form>{{end}}

<h2>Step timeline</h2>
<table>
<tr><th>Step</th><th>Kind</th><th>Status</th><th>Attempts</th><th>Spend</th><th>Ledger</th><th>Last event</th></tr>
{{range .Steps}}<tr>
  <td>{{.Name}}</td><td>{{.Kind}}</td><td>{{.Status}}</td>
  <td>{{if .Attempts}}{{.Attempts}}{{end}}</td>
  <td>{{.Spend}}</td>
  <td class="dim">{{range .SpendNotes}}{{.}}<br>{{end}}</td>
  <td class="dim">{{.At}}</td>
</tr>{{end}}
</table>

<h2>Export as replay test</h2>
<p>Teleport this incident into a failing local test:
<a href="{{.Meta.ID}}/fixture.json">fixture.json</a> ·
<a href="{{.Meta.ID}}/replay_test.go">replay_test.go skeleton</a></p>

<h2>Event log</h2>
<table>
<tr><th>#</th><th>Type</th><th>Step</th><th>Attempt</th><th>Time</th></tr>
{{range .Events}}<tr>
  <td>{{.Seq}}</td><td>{{.Type}}</td><td>{{.Step}}</td>
  <td>{{if .Attempt}}{{.Attempt}}{{end}}</td>
  <td class="dim">{{.Time.Format "15:04:05.000"}}</td>
</tr>{{end}}
</table>`))

const css = `
body{font:14px/1.5 system-ui,sans-serif;margin:2rem;max-width:70rem;color:#1a1a1a}
table{border-collapse:collapse;width:100%;margin:.5rem 0}
th,td{border:1px solid #ddd;padding:.35rem .6rem;text-align:left;vertical-align:top}
th{background:#f5f5f5}
a{color:#0645ad}
.st{padding:.1rem .5rem;border-radius:.6rem;background:#eee;font-size:.85em}
.st-completed{background:#d4edda}.st-suspended{background:#fff3cd}
.st-failed,.st-budget_exhausted,.st-incompatible,.st-rejected{background:#f8d7da}
.st-running,.st-pending,.st-waiting_retry{background:#d1ecf1}
.warn{background:#fff3cd;border:1px solid #ffeeba;padding:.6rem 1rem;border-radius:.4rem;margin:.6rem 0}
.gate{background:#f5f5f5;padding:.6rem 1rem;border-radius:.4rem}
.gate button{margin:0 .3rem;padding:.2rem .9rem}
.dim{color:#777;font-size:.85em}
`

// testTemplate is the exported skeleton. Format arguments:
// workflow, instance id (comment), sanitized id (identifier), instance id
// (filename), truncation step, truncation attempt.
const testTemplate = `package yourpackage_test

// Replay test exported by the Loom inspector for workflow %q,
// instance %q. Drop the fixture JSON next to this file, fill in the
// TODOs, and this is your production incident as a local test — no
// external services required.

import (
	"os"
	"testing"

	"github.com/IbrahimMohammedi/loom"
	"github.com/IbrahimMohammedi/loom/replay"
)

func TestReplay_%s(t *testing.T) {
	f, err := os.Open("%s_fixture.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fixture, err := replay.ReadFixture(f)
	if err != nil {
		t.Fatal(err)
	}

	// TODO: build the workflow definition exactly as production does, but
	// with MOCK dependencies — replay re-executes your step code, so every
	// side effect must be behind an injectable interface.
	var def *loom.Definition // = yourpkg.BuildWorkflow(mockDeps...)

	// TODO: a deterministic mock LLM. LLM step outputs are compared
	// structurally by default (shape, not content).
	var mockLLM loom.LLMClient

	h := replay.New(fixture.Events,
		replay.WithLLM(mockLLM),
		// Truncation defaults to the step where this incident stopped:
		replay.TruncateAt(%q, %d),
		// To test error handling instead, inject at the dependency surface:
		// replay.InjectFailure("some-step", errors.New("boom")),
	)
	res, err := h.Run(def)
	if err != nil {
		t.Fatal(err)
	}
	if res.Diverged {
		t.Fatalf("replay diverged at %%s (first divergence is the finding; later rows are advisory)", res.DivergedAt)
	}
	if res.StoppedAt != "" {
		t.Logf("stopped at %%s: %%s — script it with replay.WithResumeDecision", res.StoppedAt, res.StopReason)
	}
}
`
