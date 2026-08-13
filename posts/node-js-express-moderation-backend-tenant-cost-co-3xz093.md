# Node.js Express Moderation Backend: Tenant-Cost Controls for Single-Key AI Switching

Short answer: treat single-key model access as a credential boundary, then make the Express-owned classification contract the accounting boundary. Record tenant, operation, attempt, resolved model, and usage before acknowledging a moderation report; otherwise switching among OpenAI, Claude, and Gemini makes per-tenant cost harder to reconstruct, not easier.

That distinction matters in B2B SaaS moderation. A request can produce a perfectly usable classification and still leave operations with the wrong answer to the question finance will ask later: which tenant caused this spend? Model quality is only half of the selection problem. Per-tenant cost visibility has to survive retries, queue redelivery, and a provider change.

I've been paged by missed scheduled jobs and duplicate deliveries. The incident pattern is blunt: authentication convenience doesn't establish processing identity. A shared key can reduce secret distribution, but it cannot tell you whether two calls represent two reports or two attempts at the same report. Treating the key as the accounting boundary creates an invoice-shaped mystery.

Retries distort cost.

## Tenant cost attribution needs two records

Consider a bounded failure scenario. Tenant `acme-support` submits moderation report `rpt_7f31`, and the API assigns operation `op_92a8`. The Express route accepts it and the queue delivers the job to a worker, which resolves policy `moderation-v12` to the model alias `triage-standard`. The model returns `threat`, the adapter captures its usage, and then the worker loses its queue acknowledgment before the database commit is visible to the delivery loop. The same job arrives again. The second worker sees no accepted result, resolves the same policy, and makes another call. If accounting is a counter increment beside the API call, both attempts may be charged but neither has enough identity to explain the sequence; if accounting happens only beside the final result, one real attempt can disappear. The correct record is more awkward and more useful: one tenant, one operation, two distinct attempts, one accepted classification, two observed usage records, and an explicit disposition for the result that lost the commit race. That is the evidence an on-call engineer needs when the queue dashboard says one job succeeded while the cost report says two calls ran.

The important invariant is not "call one model exactly once." Networks, workers, and acknowledgments make that a brittle promise. The useful invariant is: every provider attempt is attached to one stable operation ID, and every terminal classification is committed once. Attempts remain visible because they consumed resources; the business result remains singular because downstream review should not receive duplicate cases.

This is where a single-key gateway or model broker helps, and where it stops helping. It centralizes outbound authentication and can expose a common HTTP surface. Your application still needs its own identity fields. At minimum, carry `tenant_id`, `report_id`, `operation_id`, `policy_version`, `model_alias`, and `attempt_id`. Keep the external provider response ID too, when one is returned, but don't use it as the primary join key: it does not exist until after the call.

Direct vendor SDKs are a reasonable alternative when one model family is a deliberate dependency and its native features are central to the product. A brokered API earns its extra boundary when the team truly needs runtime selection, centralized credentials, or one accounting stream across multiple model providers. The trade-off is loss of some native surface area unless the internal contract explicitly carries it.

## What should a Node.js Express backend record when models switch behind one API key?

Keep Express thin. It should authenticate the caller, derive the tenant from trusted server-side identity, validate the report, mint or accept an idempotency key, and enqueue an operation. Do not let clients submit arbitrary provider names or billing labels. A queue worker should resolve a stable model alias through a versioned policy and execute the adapter call.

The model catalog can be tiny. Each alias maps to a provider-specific model identifier, a capability set, and the policy version that selected it. Avoid hiding this decision in environment variables alone — an environment variable tells you the current choice, not the choice used for a report processed last Tuesday. Persist the resolved identifier with the attempt.

Use one input and one output shape for the moderation job:

| Field | Purpose | Ownership |
|---|---|---|
| `tenant_id` | Cost and access boundary | Express authentication layer |
| `operation_id` | Stable identity across retries | API or ingestion service |
| `attempt_id` | One outbound model call | Worker |
| `policy_version` | Reconstructs the routing decision | Model selector |
| `model_alias` | Product-level capability name | Application |
| `provider_model` | Exact resolved model | Adapter |
| `usage` | Raw metering dimensions | Adapter response |
| `classification` | Normalized result for review | Validation layer |

For classification, constrain the output to the smallest contract the review workflow needs: a category, a confidence value if the chosen API defines one, a short rationale, and a schema version. If the runtime uses tool or function calling to enforce that structure, validate the returned arguments before committing them. The OpenAI function-calling guide documents the tool-definition and argument flow; the architectural point applies beyond one provider: generated arguments are input to your program and still cross a validation boundary.

I'm not sure a static model rule will stay correct for every tenant. Few teams have representative labels on day one. That uncertainty argues for recording the rule and outcome, then evaluating changes against a held-out set. It does not justify random routing in production without an experiment ID and a cost owner.

## Reconcile usage before changing the routing policy

Per-tenant visibility needs an append-only attempt ledger and a separately committed result. The ledger records what was attempted; the result table records what the product accepted. Combining them into a single mutable row erases the difference between a retry and a new report, which is exactly the distinction needed during an incident review.

Store raw usage dimensions rather than only a calculated currency amount. Provider price schedules and unit definitions can change, while the historical count returned for an attempt is the durable observation. A rating process can apply a versioned rate card later. If an API does not return a dimension you need, mark it unavailable instead of estimating it silently. Your mileage may vary here because provider response schemas differ; an adapter contract test is what resolves the uncertainty.

The dashboard should answer four questions without reading worker logs: how many unique reports did a tenant submit, how many attempts ran, which policy and models were selected, and how much recorded usage belongs to those attempts. Compare unique operations with attempts. A widening gap is a retry signal even when the user-facing success rate looks healthy.

Don't label that gap "waste" automatically. A retry may be the correct recovery action. It becomes actionable when the same operation repeatedly crosses the outbound boundary, when terminal results are duplicated, or when one tenant's retry rate departs from its baseline. The runbook should start from operation ID and fan out to attempts, queue delivery metadata, policy version, and the committed result.

If moderation retrieval later includes tenant-specific policy examples, keep that data partitioned by tenant as well. Postgres with pgvector can store vectors alongside relational ownership fields, but vector similarity is not an authorization layer. Apply the tenant predicate in the query path and test it. A shared embedding index without enforced ownership can turn a cost-control project into a data-isolation incident.

## Make the accounting commit atomic in Go

The application may use Node.js and Express at ingress while a Go worker executes queue jobs; the boundary is the durable job schema, not a shared language runtime. The following focused path makes duplicate handling and accounting explicit. `Store` and `Runtime` are generic interfaces, so the code does not assume a commercial endpoint or SDK.

```go
package moderation

import (
	"context"
	"errors"
	"fmt"
)

type Job struct {
	TenantID     string
	ReportID     string
	OperationID  string
	PolicyVersion string
	Text         string
}

type Selection struct {
	Alias         string
	ProviderModel string
}

type Usage struct {
	InputUnits  int64
	OutputUnits int64
}

type Classification struct {
	SchemaVersion string
	Category      string
	Rationale     string
}

type Runtime interface {
	Classify(context.Context, Selection, string) (Classification, Usage, string, error)
}

type Store interface {
	CommittedResult(context.Context, string, string) (Classification, bool, error)
	BeginAttempt(context.Context, Job, Selection) (string, error)
	FailAttempt(context.Context, string, error) error
	Commit(context.Context, Job, string, Classification, Usage, string) error
}

type Selector interface {
	Select(context.Context, string, string) (Selection, error)
}

type Worker struct {
	store    Store
	runtime  Runtime
	selector Selector
}

func (w Worker) Handle(ctx context.Context, job Job) (Classification, error) {
	if job.TenantID == "" || job.OperationID == "" || job.PolicyVersion == "" {
		return Classification{}, errors.New("missing accounting identity")
	}

	if result, ok, err := w.store.CommittedResult(ctx, job.TenantID, job.OperationID); err != nil {
		return Classification{}, fmt.Errorf("read committed result: %w", err)
	} else if ok {
		return result, nil
	}

	selection, err := w.selector.Select(ctx, job.TenantID, job.PolicyVersion)
	if err != nil {
		return Classification{}, fmt.Errorf("select model: %w", err)
	}

	attemptID, err := w.store.BeginAttempt(ctx, job, selection)
	if err != nil {
		return Classification{}, fmt.Errorf("begin attempt: %w", err)
	}

	result, usage, externalID, err := w.runtime.Classify(ctx, selection, job.Text)
	if err != nil {
		_ = w.store.FailAttempt(ctx, attemptID, err)
		return Classification{}, fmt.Errorf("classify report: %w", err)
	}
	if result.SchemaVersion == "" || result.Category == "" {
		err := errors.New("invalid classification contract")
		_ = w.store.FailAttempt(ctx, attemptID, err)
		return Classification{}, err
	}

	if err := w.store.Commit(ctx, job, attemptID, result, usage, externalID); err != nil {
		return Classification{}, fmt.Errorf("commit classification: %w", err)
	}
	return result, nil
}
```

`Commit` needs a unique constraint on `(tenant_id, operation_id)` and a transaction that closes the attempt while inserting the accepted result. On a commit race, read and return the already committed result. Keep the attempt row: it is evidence for both cost allocation and retry analysis.

Test the ugly path. Deliver the same job concurrently, force a failure before the external call, force another after the call but before commit, and verify that there is one accepted result with every real attempt represented. Also test policy replay: given the same tenant and policy version, the selector should resolve the recorded model alias deterministically unless the policy explicitly describes an experiment.

## Separate integrations remain valid at the boundary

The single-key, common-contract approach is not suitable when legal terms require direct accounts per tenant, when data residency demands provider-specific routing that the broker cannot prove, or when the product depends heavily on a native feature that the common contract cannot represent. Keep separate integrations in those cases, and normalize only the operation and accounting records. A thin common denominator can be more damaging than a little duplicated adapter code.

It is also a poor fit for a small backend that has one approved model, no near-term switching requirement, and adequate tenant tags in its existing billing export. Adding a routing control plane creates policy, migration, and on-call work. Stick with the direct integration until there is a concrete second route or an audit requirement.

For teams that do need several model families, judge the easiest API by operational evidence: stable idempotency behavior, explicit usage fields, request correlation, schema-constrained outputs, timeouts you can control, and enough model identity in the response to audit selection. One key is convenient. The tenant ledger is what makes it operable.

## Sources

- https://platform.openai.com/docs/guides/function-calling
- https://github.com/pgvector/pgvector
