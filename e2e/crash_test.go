//go:build unix

// Package e2e holds the canonical crash-recovery tests (ARCHITECTURE.md,
// Testing strategy): the real triage demo run as a subprocess and killed
// with real signals — not an in-process simulateCrash hook, which would
// test a politeness the real world does not have.
package e2e

import (
	"bufio"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/IbrahimMohammedi/loom"
	"github.com/IbrahimMohammedi/loom/store/sqlite"
)

var (
	buildOnce sync.Once
	binPath   string
	buildErr  error
)

func triageBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "loom-e2e-*")
		if err != nil {
			buildErr = err
			return
		}
		binPath = filepath.Join(dir, "triage")
		cmd := exec.Command("go", "build", "-o", binPath, ".")
		cmd.Dir = "../examples/triage"
		if out, err := cmd.CombinedOutput(); err != nil {
			buildErr = err
			t.Logf("build output: %s", out)
		}
	})
	if buildErr != nil {
		t.Fatalf("building triage demo: %v", buildErr)
	}
	return binPath
}

// proc wraps a running triage subprocess with line-scanned stdout.
type proc struct {
	cmd     *exec.Cmd
	lines   chan string
	allOut  *strings.Builder
	inLLM   chan struct{}
	inLLMmu sync.Once
}

func startTriage(t *testing.T, bin string, args ...string) *proc {
	t.Helper()
	p := &proc{
		cmd:    exec.Command(bin, args...),
		lines:  make(chan string, 256),
		allOut: &strings.Builder{},
		inLLM:  make(chan struct{}),
	}
	p.cmd.Stderr = os.Stderr
	stdout, err := p.cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			line := sc.Text()
			p.allOut.WriteString(line + "\n")
			if strings.Contains(line, "LLM_CALL_IN_FLIGHT") {
				p.inLLMmu.Do(func() { close(p.inLLM) })
			}
			select {
			case p.lines <- line:
			default:
			}
		}
		close(p.lines)
	}()
	return p
}

func (p *proc) waitInLLM(t *testing.T) {
	t.Helper()
	select {
	case <-p.inLLM:
	case <-time.After(20 * time.Second):
		t.Fatalf("subprocess never reached the LLM step; output:\n%s", p.allOut.String())
	}
}

// exitCode waits for the process and returns its exit code.
func (p *proc) exitCode(t *testing.T) int {
	t.Helper()
	err := p.cmd.Wait()
	if err == nil {
		return 0
	}
	if ee, ok := err.(*exec.ExitError); ok {
		return ee.ExitCode()
	}
	t.Fatalf("wait: %v", err)
	return -1
}

func countEvents(t *testing.T, db, id string, typ loom.EventType, step string) int {
	t.Helper()
	store, err := sqlite.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	events, err := store.Load(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, ev := range events {
		if ev.Type == typ && (step == "" || ev.Step == step) {
			n++
		}
	}
	return n
}

func instanceStatus(t *testing.T, db, id string) string {
	t.Helper()
	store, err := sqlite.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	meta, err := store.Meta(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return meta.Status
}

// TestCrashSafeResume is the canonical test: SIGKILL the demo inside the
// LLM step (reservation appended, result not), restart a fresh process
// against the same file, and prove the fold recovers — completed steps not
// re-executed, orphan settled at reserved max, execution continuing.
func TestCrashSafeResume(t *testing.T) {
	bin := triageBinary(t)
	db := filepath.Join(t.TempDir(), "state.db")

	p := startTriage(t, bin, "-db", db, "-id", "t1", "-linger", "30s", "run")
	p.waitInLLM(t)
	if err := p.cmd.Process.Kill(); err != nil { // SIGKILL: no grace, no drain
		t.Fatal(err)
	}
	_ = p.cmd.Wait()

	// The killed process left: classify + fetch-context completed, draft
	// started with an open reservation.
	if got := countEvents(t, db, "t1", loom.EvStepCompleted, "classify"); got != 1 {
		t.Fatalf("pre-resume: classify completions = %d", got)
	}
	if got := countEvents(t, db, "t1", loom.EvBudgetSettled, "draft"); got != 0 {
		t.Fatalf("pre-resume: reservation must be open, settlements = %d", got)
	}

	// Fresh process, same file, same code path.
	out, err := exec.Command(bin, "-db", db, "-id", "t1", "run").CombinedOutput()
	if err != nil {
		t.Fatalf("resume run failed: %v\n%s", err, out)
	}
	text := string(out)
	if !strings.Contains(text, "recovered from log") {
		t.Fatalf("resume output missing recovery markers:\n%s", text)
	}
	if !strings.Contains(text, "crash-orphaned") {
		t.Fatalf("resume output missing orphan settlement:\n%s", text)
	}

	// Completed steps executed exactly once across both processes.
	for _, step := range []string{"classify", "fetch-context"} {
		if got := countEvents(t, db, "t1", loom.EvStepStarted, step); got != 1 {
			t.Fatalf("step %s started %d times; completed steps must not re-execute", step, got)
		}
	}
	// Draft: attempt 1 (killed) + attempt 2 (resumed) = 2 starts, 1 completion.
	if got := countEvents(t, db, "t1", loom.EvStepStarted, "draft"); got != 2 {
		t.Fatalf("draft starts = %d, want 2 (killed attempt + retry)", got)
	}
	if got := countEvents(t, db, "t1", loom.EvStepCompleted, "draft"); got != 1 {
		t.Fatalf("draft completions = %d", got)
	}
	// Ledger: orphan settled at max + actual settle for attempt 2.
	if got := countEvents(t, db, "t1", loom.EvBudgetSettled, "draft"); got != 2 {
		t.Fatalf("draft settlements = %d, want 2 (crash-orphaned + actual)", got)
	}
	if got := instanceStatus(t, db, "t1"); got != loom.StatusSuspended {
		t.Fatalf("instance should be suspended at the gate, got %s", got)
	}

	// Complete the workflow: approve, run again.
	if out, err := exec.Command(bin, "-db", db, "-id", "t1", "approve").CombinedOutput(); err != nil {
		t.Fatalf("approve: %v\n%s", err, out)
	}
	if out, err := exec.Command(bin, "-db", db, "-id", "t1", "run").CombinedOutput(); err != nil {
		t.Fatalf("final run: %v\n%s", err, out)
	}
	if got := instanceStatus(t, db, "t1"); got != loom.StatusCompleted {
		t.Fatalf("want completed, got %s", got)
	}
}

// TestZombieFencing is the only real test of ADR-8: SIGSTOP a process
// mid-step, let a second process claim the instance (epoch bump), SIGCONT
// the zombie, and assert its late append is rejected by the store — exit
// code 3 — with exactly one completion for the contested step.
func TestZombieFencing(t *testing.T) {
	bin := triageBinary(t)
	db := filepath.Join(t.TempDir(), "state.db")

	zombie := startTriage(t, bin, "-db", db, "-id", "z1", "-linger", "2s", "run")
	zombie.waitInLLM(t)
	if err := zombie.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}

	// Usurper claims the instance, settles the orphan, re-runs draft,
	// suspends at the gate.
	out, err := exec.Command(bin, "-db", db, "-id", "z1", "run").CombinedOutput()
	if err != nil {
		t.Fatalf("usurper run failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "crash-orphaned") {
		t.Fatalf("usurper must settle the zombie's reservation:\n%s", out)
	}

	// Wake the zombie. Its linger timer expired on the wall clock while
	// stopped, so it returns immediately and tries to append with the old
	// epoch — the store must reject it.
	if err := zombie.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	if code := zombie.exitCode(t); code != 3 {
		t.Fatalf("zombie exit code = %d, want 3 (stale epoch); output:\n%s", code, zombie.allOut.String())
	}

	// Two results for the contested step cannot both land (ADR-8): the
	// usurper's completion is the only one.
	if got := countEvents(t, db, "z1", loom.EvStepCompleted, "draft"); got != 1 {
		t.Fatalf("draft completions = %d, want exactly 1", got)
	}
	if got := instanceStatus(t, db, "z1"); got != loom.StatusSuspended {
		t.Fatalf("instance should be suspended at the gate, got %s", got)
	}
}
