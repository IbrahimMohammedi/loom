package inspector_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/IbrahimMohammedi/loom"
	"github.com/IbrahimMohammedi/loom/inspector"
	"github.com/IbrahimMohammedi/loom/replay"
	"github.com/IbrahimMohammedi/loom/store/memory"
)

type mockLLM struct{}

func (mockLLM) Complete(ctx context.Context, req loom.LLMRequest) (loom.LLMResponse, error) {
	return loom.LLMResponse{Model: req.Model, Text: "draft", InputTokens: 10, OutputTokens: 5}, nil
}

func setup(t *testing.T) (*memory.Store, *loom.Engine, *loom.Definition) {
	t.Helper()
	store := memory.New()
	wf := loom.NewWorkflow("wf")
	wf.Step("classify", loom.Typed(func(ctx context.Context, sctx *loom.StepContext, in string) (string, error) {
		return "billing", nil
	}))
	wf.LLMStep("draft", []loom.ModelSpec{{Model: "m", MaxInputTokens: 100, MaxOutputTokens: 100, InPrice: 100, OutPrice: 100}},
		loom.Typed(func(ctx context.Context, sctx *loom.StepContext, in string) (string, error) {
			resp, err := sctx.LLM().Complete(ctx, in)
			if err != nil {
				return "", err
			}
			return resp.Text, nil
		}))
	wf.Approval("gate")
	def, err := wf.Build()
	if err != nil {
		t.Fatal(err)
	}
	eng, err := loom.NewEngine(loom.Config{Store: store, LLM: mockLLM{}, ExecutorID: "t", Tick: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	eng.Register(def)
	return store, eng, def
}

func TestInspectorPages(t *testing.T) {
	ctx := context.Background()
	store, eng, def := setup(t)
	_ = eng.StartInstance(ctx, "wf", "i1", "ticket", loom.Dollars(1))
	_ = eng.RunInstance("i1") // suspends at gate

	insp := inspector.New(store, map[string]*loom.Definition{"wf": def})
	srv := httptest.NewServer(insp)
	defer srv.Close()

	body := get(t, srv.URL+"/")
	if !strings.Contains(body, "i1") || !strings.Contains(body, "suspended") {
		t.Fatalf("list page:\n%s", body)
	}

	body = get(t, srv.URL+"/instance/i1")
	for _, want := range []string{"classify", "draft", "gate", "awaiting approval", "settled", "replay_test.go"} {
		if !strings.Contains(body, want) {
			t.Fatalf("detail page missing %q:\n%s", want, body)
		}
	}

	// Manual resume from the UI: same constrained-append path as a webhook.
	resp, err := http.PostForm(srv.URL+"/instance/i1/resume", map[string][]string{"approved": {"true"}})
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	// Second decision loses, loudly.
	resp2, err := http.PostForm(srv.URL+"/instance/i1/resume", map[string][]string{"approved": {"false"}})
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusConflict {
		t.Fatalf("racing resume must 409, got %d", resp2.StatusCode)
	}

	// Export: fixture parses and drives the replay harness.
	fresp, err := http.Get(srv.URL + "/instance/i1/fixture.json")
	if err != nil {
		t.Fatal(err)
	}
	defer fresp.Body.Close()
	fixture, err := replay.ReadFixture(fresp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if fixture.InstanceID != "i1" || len(fixture.Events) == 0 {
		t.Fatalf("bad fixture: %+v", fixture)
	}
	h := replay.New(fixture.Events, replay.WithLLM(mockLLM{}), replay.TruncateAt("draft", 1))
	res, err := h.Run(def)
	if err != nil {
		t.Fatal(err)
	}
	if res.Diverged {
		t.Fatalf("exported fixture must replay cleanly: %+v", res)
	}

	body = get(t, srv.URL+"/instance/i1/replay_test.go")
	for _, want := range []string{"replay.New", `replay.TruncateAt("gate", 1)`, "replay.ReadFixture"} {
		if !strings.Contains(body, want) {
			t.Fatalf("skeleton missing %q:\n%s", want, body)
		}
	}
}

func get(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var b strings.Builder
	buf := make([]byte, 32*1024)
	for {
		n, err := resp.Body.Read(buf)
		b.Write(buf[:n])
		if err != nil {
			break
		}
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: %d\n%s", url, resp.StatusCode, b.String())
	}
	return b.String()
}
