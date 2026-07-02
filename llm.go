package loom

import (
	"context"
	"errors"
	"fmt"
)

type LLMRequest struct {
	Model           string
	Prompt          string
	MaxOutputTokens int
}

type LLMResponse struct {
	Model        string
	Text         string
	InputTokens  int
	OutputTokens int
}

// LLMClient is the pluggable provider seam. MVP ships no real provider;
// the mock in examples/triage and any real SDK sit behind this interface.
type LLMClient interface {
	Complete(ctx context.Context, req LLMRequest) (LLMResponse, error)
}

// ModelSpec is one hop in a fallback chain, with the price data the budget
// ledger needs. Prices are Cost (nano-dollars) per token.
type ModelSpec struct {
	Model           string
	MaxInputTokens  int
	MaxOutputTokens int
	InPrice         Cost
	OutPrice        Cost
}

// MaxCost is the worst case for one call to this hop.
func (m ModelSpec) MaxCost() Cost {
	return Cost(m.MaxInputTokens)*m.InPrice + Cost(m.MaxOutputTokens)*m.OutPrice
}

func (m ModelSpec) costOf(r LLMResponse) Cost {
	return Cost(r.InputTokens)*m.InPrice + Cost(r.OutputTokens)*m.OutPrice
}

// chainMaxCost is the reservation for the whole chain: the SUM of per-hop
// maxes, not the max (ADR-6) — every hop before the last can bill partially
// and the last bills fully. A 3-model chain is honestly a 3x worst-case
// liability, visible at configuration time.
func chainMaxCost(chain []ModelSpec) Cost {
	var t Cost
	for _, m := range chain {
		t += m.MaxCost()
	}
	return t
}

// ChainCaller executes prompts against a fallback chain, accumulating the
// real cost of every attempted hop — failed hops bill whatever the provider
// reports (input tokens always, partial output sometimes).
type ChainCaller struct {
	client LLMClient
	chain  []ModelSpec
	spent  Cost
}

func (c *ChainCaller) Complete(ctx context.Context, prompt string) (LLMResponse, error) {
	if c.client == nil {
		return LLMResponse{}, Permanent(errors.New("loom: no LLMClient configured (set Config.LLM on the engine, or the harness's WithLLM)"))
	}
	var errs []error
	for _, m := range c.chain {
		resp, err := c.client.Complete(ctx, LLMRequest{Model: m.Model, Prompt: prompt, MaxOutputTokens: m.MaxOutputTokens})
		c.spent += m.costOf(resp)
		if err == nil {
			return resp, nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", m.Model, err))
	}
	return LLMResponse{}, fmt.Errorf("loom: all models in chain failed: %w", errors.Join(errs...))
}

// Spent is the accumulated actual cost across all hops attempted so far.
func (c *ChainCaller) Spent() Cost { return c.spent }
