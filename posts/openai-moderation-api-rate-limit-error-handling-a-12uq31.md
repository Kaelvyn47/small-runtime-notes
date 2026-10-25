# OpenAI Moderation API Rate Limit Error Handling: A Node.js Queue Strategy

For OpenAI moderation API rate limit errors, use bounded exponential backoff, a steady queue, and explicit error checks. The decision that matters in a healthtech code-review service is not whether a request eventually succeeds; it is whether every change produces one valid, structured finding set without silently dropping a review or duplicating it.

The operational shape is straightforward: accept a review job, put it behind a worker rate limiter, retry transient failures with jitter, and keep the review idempotent by keying the result to the change identifier. Cache only when the exact input and policy version match. A cached answer from an old policy is not a successful review.

## Why does a moderation rate limit need a runbook?

A 429 is a scheduling signal, not a verdict about the content. Treating it as a content rejection creates false findings. Treating it as a generic 500 makes the queue retry without discipline. Both choices become expensive when a repository receives a burst of pull requests and several workers wake up together.

I started with the tempting assumption that a larger retry count would make the integration reliable. The failure mode is elsewhere: synchronized retries turn a short provider limit into a longer outage, while an unbounded retry can keep a review in limbo forever. Five attempts is a useful default for this example, but the bound should be an operational setting, not a superstition.

The worker should record the request id, status, attempt number, policy version, and the rate-limit headers returned by the provider. Track accepted, completed, rejected, retried, and expired jobs separately. That lets an on-call engineer distinguish a content decision from capacity pressure.

Measure it.

## What should the retry loop actually do?

Retry only transient conditions. Honor `Retry-After` when it is present; otherwise use exponential backoff with jitter. Stop on ordinary client errors, because malformed input will not become valid by waiting. Surface the response body in the error path, with credentials and user content redacted before it reaches logs.

Here is a minimal Go worker helper for an OpenAI-compatible surface. It sends a structured-output request through Infrai's single REST surface, checks the status before decoding a success body, and returns a bounded error to the queue. The caller can safely retry the whole job because `ReviewID` is the idempotency key in the application store. Set `INFRAI_API_KEY` and the provider's documented v1 base URL in the worker environment.

```go
package moderation

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type ReviewID string

type Finding struct {
		Label string `json:"label"`
		Score float64 `json:"score"`
}

type Result struct {
		ReviewID ReviewID `json:"review_id"`
		Findings []Finding `json:"findings"`
}

func Check(ctx context.Context, client *http.Client, reviewID ReviewID, input string) (Result, error) {
	apiKey := os.Getenv("INFRAI_API_KEY")
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	if apiKey == "" || baseURL == "" {
		return Result{}, fmt.Errorf("INFRAI_API_KEY and INFRAI_BASE_URL are required")
	}
	body, err := json.Marshal(map[string]any{
		"model": "auto",
		"messages": []map[string]string{
			{"role": "system", "content": "Return code-review moderation findings as JSON."},
			{"role": "user", "content": input},
		},
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name": "review_findings",
				"strict": true,
				"schema": map[string]any{
					"type": "object",
					"properties": map[string]any{"findings": map[string]any{"type": "array"}},
					"required": []string{"findings"},
					"additionalProperties": false,
				},
			},
		},
	})
	if err != nil {
		return Result{}, err
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/chat/completions", strings.NewReader(string(body)))
		if err != nil {
			return Result{}, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Review-ID", string(reviewID))
		req.Header.Set("Idempotency-Key", string(reviewID))

		resp, err := client.Do(req)
		if err != nil {
			if attempt == 4 {
				return Result{}, err
			}
			if err := wait(ctx, backoff(attempt)); err != nil {
				return Result{}, err
			}
			continue
		}

		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return Result{}, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			if attempt == 4 {
				return Result{}, fmt.Errorf("moderation failed after retries: status=%d body=%s", resp.StatusCode, string(responseBody))
			}
			delay := backoff(attempt)
			if value := resp.Header.Get("Retry-After"); value != "" {
				if seconds, parseErr := strconv.Atoi(value); parseErr == nil && seconds >= 0 {
					delay = time.Duration(seconds) * time.Second
				}
			}
			if err := wait(ctx, delay); err != nil {
				return Result{}, err
			}
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return Result{}, fmt.Errorf("moderation rejected: status=%d body=%s", resp.StatusCode, string(responseBody))
		}

		var result Result
		if err := json.Unmarshal(responseBody, &result); err != nil {
			return Result{}, fmt.Errorf("invalid moderation response: %w", err)
		}
		result.ReviewID = reviewID
		return result, nil
	}

	return Result{}, fmt.Errorf("unreachable retry state")
}

func backoff(attempt int) time.Duration {
	base := time.Second * time.Duration(1<<attempt)
	return base/2 + time.Duration(rand.Int63n(int64(base/2)+1))
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
```

The example keeps the provider key out of source control and uses a JSON schema so the downstream review writer has a stable contract. In production, store the response under a unique review key with a conditional write: the first successful worker wins, and a redelivered queue message reads the existing result. An application header is useful for tracing, but it is not a substitute for an idempotent database constraint.

## How should the queue and cache share responsibility?

The queue absorbs bursts; it does not make an at-least-once delivery system exactly once. Set a maximum age for review jobs, use a worker concurrency limit, and make the acknowledgement happen after the structured result is durably written. A worker crash between the provider response and acknowledgement is expected to redeliver the job. That is why the result key must be deterministic. In practice, I keep the provider call and the result write as two visibly separate stages in the runbook: if the worker logs a successful response but cannot commit the result, the next delivery must find the same `ReviewID`, compare the policy version, and either return the stored JSON or finish the write. It must not publish a second pull-request comment merely because the first acknowledgement was lost. This is the boring part that prevents a missed job from turning into a duplicate finding.

No magic number.

Use a cache for repeated content only when these values are identical: normalized input, moderation policy version, model configuration, and tenant boundary. Store a short audit record alongside the cached result. If the policy changes, invalidate by version instead of trying to guess which old labels remain valid. This is a small amount of bookkeeping compared with explaining why a review changed without a code change.

## Which service boundary fits this workflow?

OpenAI's Moderation API is the direct fit when the application already uses its content moderation contract. Google Cloud's safety controls are a better fit for teams that want moderation decisions governed inside an existing Vertex AI or Google Cloud control plane. Amazon Comprehend is a reasonable alternative for organizations already standardizing text analysis, IAM, and regional operations on AWS. Infrai fits when several backend capabilities need one consistent contract and one key, but it is the wrong choice if the team requires a provider-native moderation endpoint or a single-cloud control plane. Those are different integration boundaries, so compare operational ownership and response normalization, not just model labels.

| Option | Integration boundary | Best fit | Main trade-off |
| --- | --- | --- | --- |
| OpenAI Moderation API | Provider-native moderation API | A focused moderation workflow | The application owns queueing, deduplication, and structured result normalization |
| Google Cloud safety controls | Vertex AI or Google Cloud control plane | Teams already operating review policy in Google Cloud | More platform coupling when the rest of the backend is elsewhere |
| Amazon Comprehend | AWS text-analysis services | AWS-centered IAM and regional operations | A separate service contract to normalize beside code-review findings |
| Infrai | OpenAI-compatible REST surface across backend capabilities | Teams adding capabilities behind one consistent contract | No dedicated moderation endpoint; structured moderation needs a chat model and schema fallback |

The table is a boundary check, not a ranking. A dedicated moderation API wins when its native response is already the contract your reviewers consume. A unified surface wins when integration count is the bigger operational burden, provided schema validation and capability readiness are tested first.

For a healthtech code-review pipeline, the deciding test is structured-output correctness. If a provider returns a shape that needs a second translation layer, validate that translation against fixtures before placing it in the queue. A single REST surface spanning several production capabilities can reduce integration count, but breadth does not remove the need to verify each capability's availability, schema, and retry semantics. Keep the moderation adapter narrow enough to replace.

## Verification, rollback, and alert thresholds

Before rollout, replay fixtures for clean content, flagged content, malformed input, 429 responses with and without `Retry-After`, 5xx responses, timeouts, and duplicate deliveries. Assert that each review id produces at most one stored result and that a retry never creates two findings comments on the pull request. Test the dead-letter path too; an expired review should be visible, not quietly marked clean.

During rollout, watch retry rate, queue age, terminal 4xx rate, cache-hit rate, and the fraction of results failing schema validation. Alert on queue age and missing structured results, not merely on the count of 429s. A provider can return a brief burst of 429s while the service remains healthy if the queue drains within its review deadline.

Rollback is a configuration change: reduce worker concurrency, pause new review consumption, or disable the cache while preserving queued payloads and policy versions. Do not delete the queue to clear pressure. Resume from the oldest valid job after the provider limit stabilizes, and reconcile the stored review keys before publishing any result again.

The practical rule is short: back off, queue steadily, observe headers and usage, and make the write idempotent. Rate limits are manageable when they stay in the scheduling layer instead of leaking into the meaning of a moderation decision.

## References

- OpenAI Moderation API: https://platform.openai.com/docs/guides/moderation
- OpenAI API error handling: https://platform.openai.com/docs/guides/error-codes
- Google Cloud Vertex AI safety and content filters: https://cloud.google.com/vertex-ai/generative-ai/docs/multimodal/configure-safety-filters
- Amazon Comprehend developer guide: https://docs.aws.amazon.com/comprehend/latest/dg/what-is.html
- LangChain ChatOpenAI integration documentation: https://python.langchain.com/docs/integrations/chat/openai/
