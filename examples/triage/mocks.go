package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/IbrahimMohammedi/loom"
)

// Chain is the demo's fallback chain, priced from a static table of real
// model rates (per-token, in nano-dollars): quill-large at $3/M input and
// $15/M output; quill-mini at $0.25/M and $1.25/M. The reservation the
// ledger takes is the SUM of both hops' maxes (ADR-6).
var Chain = []loom.ModelSpec{
	{Model: "quill-large", MaxInputTokens: 2000, MaxOutputTokens: 1000, InPrice: 3000, OutPrice: 15000},
	{Model: "quill-mini", MaxInputTokens: 2000, MaxOutputTokens: 1000, InPrice: 250, OutPrice: 1250},
}

// MockLLM bills realistic tokens: input derived from the actual prompt
// (chars/4), output from the actual generated text. Round-number fake costs
// would make the ledger demo read as theater; boring and real is the spec.
type MockLLM struct {
	// Linger holds the "call" open, giving crash tests a window to kill or
	// stop the process mid-step.
	Linger time.Duration
	// FailLarge simulates the primary dying with a 529 AFTER accepting
	// input tokens — a failed hop is not free.
	FailLarge bool
}

func (m *MockLLM) Complete(ctx context.Context, req loom.LLMRequest) (loom.LLMResponse, error) {
	inTokens := len(req.Prompt) / 4
	if m.Linger > 0 {
		fmt.Println("LLM_CALL_IN_FLIGHT")
		select {
		case <-time.After(m.Linger):
		case <-ctx.Done():
			return loom.LLMResponse{Model: req.Model, InputTokens: inTokens}, ctx.Err()
		}
	}
	if m.FailLarge && req.Model == "quill-large" {
		return loom.LLMResponse{Model: req.Model, InputTokens: inTokens},
			errors.New("simulated 529 overloaded")
	}
	text := draftFor(req.Prompt)
	return loom.LLMResponse{
		Model: req.Model, Text: text,
		InputTokens: inTokens, OutputTokens: len(text) / 4,
	}, nil
}

func draftFor(prompt string) string {
	topic := "your request"
	if i := strings.Index(prompt, "category: "); i >= 0 {
		topic = strings.TrimSpace(prompt[i+len("category: "):])
		if j := strings.IndexByte(topic, '\n'); j >= 0 {
			topic = topic[:j]
		}
	}
	return fmt.Sprintf(
		"Hi — thanks for reaching out about %s. I've looked into your account "+
			"and here is what I found. We've applied the correction on our side; "+
			"you should see it reflected within one business day. If anything "+
			"still looks off, reply to this email and it will come straight back "+
			"to me.", topic)
}

// MockMailer stands in for the email provider. It honors the step-derived
// idempotency key (ADR-2): re-sends with a key it has already seen are
// dropped, which is how an at-least-once step makes its side effect safe.
type MockMailer struct {
	sent map[string]bool
}

func NewMockMailer() *MockMailer { return &MockMailer{sent: map[string]bool{}} }

func (m *MockMailer) Send(idempotencyKey, to, body string) error {
	if m.sent[idempotencyKey] {
		fmt.Printf("  mailer: duplicate send suppressed (idempotency key %s)\n", idempotencyKey[:8])
		return nil
	}
	m.sent[idempotencyKey] = true
	fmt.Printf("  mailer: sent to %s (%d bytes, key %s)\n", to, len(body), idempotencyKey[:8])
	return nil
}

// MockKB is the fetch-context dependency: a canned knowledge-base lookup.
type MockKB struct{}

func (MockKB) Lookup(category string) string {
	return fmt.Sprintf("KB[%s]: known issue, workaround available, SLA 1 business day", category)
}
