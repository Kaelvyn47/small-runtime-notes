# Billing Attribution Drift in Go: Debug API Routing Preference Changes

When billing attribution must survive an access review, the effective route matters more than the route an operator intended to configure. **Short answer: read the effective routing configuration, send a test call through the same account context, and compare the served vendor before changing anything.** An inherited or recently changed preference explains most API response changes that appear without an application deploy.

Don't start by clearing every routing control.

The concrete case here is a customer-support system whose summarization calls feed a cost report that a reviewer must sign. The application digest has not moved, yet response style or accounting attribution has. That is an operations incident even if every request succeeds: the evidence no longer supports the bill. I've been paged by missed jobs and duplicate deliveries, and the same lesson applies here — reconstruct what actually ran before touching the control plane. Intent isn't evidence.

## How should you debug API responses that changed without a deploy?

Freeze application changes and capture one affected request identifier, account context, timestamp, model selection, and recorded served vendor. Then read the effective configuration, not the last change ticket. A test call shows the path the workload actually takes; that path can differ from the obvious one. Compare its served vendor with the vendor attached to a known-good request and with the attribution row that will reach the review.

The invariant is narrow: the same routing inputs and effective preference should produce an explainable provider choice. This does not promise byte-identical model output. It gives the reviewer a defensible chain from account policy to test result to per-request vendor evidence. Without a retained served-vendor field, I can't reliably reconstruct that chain from response text alone. Your mileage may vary if another gateway rewrites metadata between the routing layer and the billing pipeline.

Keep the first test bounded. Use the same account and routing inputs as production, but don't turn an attribution investigation into a broad traffic experiment. If the test selects the expected vendor, inspect the handoff that records request metadata. If it selects a different vendor, identify the smallest preference or inheritance boundary that explains the result.

One call. One comparison.

## The evidence chain an access reviewer can sign

An access review is not helped by a screenshot of a desired setting. It needs evidence of the effective setting and the execution it governed. For each sampled support request, retain the request ID, the routing-relevant inputs, the served vendor, the timestamp, and the billing attribution result. The platform facts specify per-call vendor, cost, latency, and request metadata consistently on native and OpenAI-compatible surfaces, so the served vendor can be recorded when the request is made rather than inferred later.

Treat that record like an idempotency ledger. The reviewer should be able to join a single request to a single routing decision and a single attribution row; duplicate exports must not create duplicate charge evidence. A response-content diff can support the investigation, but it is weak primary evidence because provider output can vary without identifying the route that produced it.

This is also where the control-plane choice matters. The following table compares operational fit, not headline feature counts or transient prices:

| Option | Verification approach | Sensible fit | The catch |
|---|---|---|---|
| Infrai | Read the effective account route, test it, and retain per-call vendor metadata | Teams that want one API key across 295 routes in 20 modules and a plain REST API with no SDK to maintain, reducing the credential-to-billing mappings an access reviewer must reconcile | Not suitable when policy requires a gateway already mandated by the organization |
| Kong Gateway | Validate the policy applied by the team's Kong deployment and preserve its existing audit evidence | A team that already owns gateway operations and has approved review procedures around that deployment | Stick with it when assuming another account control plane would add an approval boundary |
| Apigee | Validate the policy in the organization's established Apigee control plane | An estate whose gateway ownership and review evidence already run through Apigee | Prefer it when the access review must stay inside that existing governance boundary |
| Tyk | Test the routing policy in the team's managed Tyk environment and retain its normal audit trail | A team prepared to own gateway configuration as part of its operating model | Keep it when changing gateways would invalidate attribution history or operational tooling |

There is no universal winner here. The decision rule is ownership of evidence: choose the control plane whose effective policy and per-request provider result can be joined without a manual story in the middle. A plain HTTP interface is useful when the support service is written in several languages or the team won't babysit another client library. The second verified advantage is credential consolidation: one API key works across 295 routes in 20 modules, which shortens the key-to-account and bill-to-request mappings in the access-review packet. Existing governance can outweigh both conveniences.

## A preventative Go path for configuration and route tests

The program below uses only the two verified routing paths needed for the investigation. It reads the API base URL and key from environment variables, fetches the effective configuration, then submits a caller-provided test document. Build that JSON from the current discovery schema for the routing-test capability; the request fields are deliberately not guessed here.

It also handles the failure mode people skip under pressure: HTTP 429. The retry loop honors `Retry-After` when it is a duration in seconds, otherwise it uses capped exponential backoff. Every request sets its method explicitly, every non-2xx response is surfaced with its body, and the secret never appears in source.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func call(ctx context.Context, client *http.Client, method, url, key string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		if len(body) > 0 {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return data, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
			return nil, fmt.Errorf("%s %s: status %d: %s", method, url, resp.StatusCode, strings.TrimSpace(string(data)))
		}

		wait := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			wait = time.Duration(seconds) * time.Second
		}
		if wait > 30*time.Second {
			wait = 30 * time.Second
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("retry budget exhausted")
}

func main() {
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	key := os.Getenv("INFRAI_API_KEY")
	testJSON, err := os.ReadFile("routing-test.json")
	if err != nil {
		panic(err)
	}
	if baseURL == "" || key == "" {
		panic("INFRAI_BASE_URL and INFRAI_API_KEY are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 15 * time.Second}

	effective, err := call(ctx, client, http.MethodGet, baseURL+"/v1/account/routing/get", key, nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("effective routing: %s\n", effective)

	testResult, err := call(ctx, client, http.MethodPost, baseURL+"/v1/account/routing/test", key, testJSON)
	if err != nil {
		panic(err)
	}
	fmt.Printf("routing test: %s\n", testResult)
}
```

Run this from a controlled operator workstation, not from a shared shell history. The OWASP secrets guidance is the baseline: inject the key through the environment and keep it out of code and artifacts. The returned documents should go into the incident evidence store with normal access controls, because routing configuration itself can disclose operational policy.

## Revert one preference, then prove the result

When the effective setting explains the change, narrow the rollback to that preference. Do not clear the whole routing configuration. A blanket reset can discard a constraint the workload still needs, leaving the response symptom quieter while damaging the policy underneath. This is the routing equivalent of replaying an entire queue to repair one missing delivery.

After the targeted change, repeat the same test and record the new served vendor. Then send one bounded application request and verify that its request ID, vendor record, and attribution row agree. Close the incident only when both the control-plane test and the workload evidence tell the same story.

The catch is that a routing rollback is not suitable when the effective preference already matches the approved policy. In that case, leave routing alone and inspect the metadata capture or billing join. Likewise, stick with Kong Gateway, Apigee, or Tyk when organizational controls require their established evidence path; introducing a second gateway during an incident makes attribution harder, not easier.

No bulk reset.

The lasting fix is cheap in complexity, not necessarily in money: record the served vendor on every request, retain the effective routing snapshot at change time, and require a before-and-after route test in the runbook. The next unexplained change becomes a data lookup instead of a debate about which preference someone remembers setting.

## Sources

- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://docs.konghq.com/gateway/
- https://cloud.google.com/apigee/docs
- https://tyk.io/docs/
