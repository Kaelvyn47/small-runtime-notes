# Node.js Password Reset Email Reliability — DKIM, SPF, Suppression, and Bounce Controls

Short answer: release a Node.js SaaS password reset flow only when the sending domain is verified, DKIM and SPF are in place, suppression is checked before every attempt, and bounce outcomes can be reconciled to one application request.

For a fintech marketplace, a seller locked out of an account needs one usable reset message. Three ambiguous attempts are worse than one delayed attempt. Provider selection matters, but the release invariant comes first.

## How do you evaluate Node.js SaaS password reset email deliverability?

Write four invariants into the deployment check: the provider reports the sending domain as verified; the expected DKIM and SPF records are published; the test recipient is not suppressed; and the application can join a reset request, queue job, provider message ID, and observed outcome under one stable ID. A successful send response alone doesn't satisfy the gate. It says nothing about inbox arrival, suppression, or whether an uncertain retry creates another valid token.

The public endpoint must return the same response for known and unknown accounts. It should create a random, single-use, expiring token, apply rate limits, and avoid response differences that enable account enumeration. Those are application controls. DKIM signs the message and SPF identifies permitted sending infrastructure; those are domain controls. Keep the boundary explicit because a green domain check cannot repair unsafe token semantics, and a perfect token cannot authenticate mail.

Treat an unexpected verification state or a stale delivery poller as a failed release condition. Email events are pull-based here, with no webhook event stream, so freshness depends on an owned polling cadence. Your mileage may vary on DNS propagation timing — caches and resolvers intervene — but the deployment decision can still be deterministic: read provider state and block until it matches the runbook.

Block the release.

## Integrate domain trust with the reset-token ledger

The first timeline is security state: request accepted, token issued, token superseded or consumed, token expired. The second is delivery state: job queued, suppression checked, send attempted, provider ID stored, bounce or delivery outcome observed. Join them with the application ID, but don't collapse them. A mailbox outcome must never reactivate an expired token, and a network retry must never mint a fresh one.

This split handles the ugly case cleanly. Suppose the worker submits a message and loses the response. It may repeat the same logical operation under a retry-safe identity, but it cannot assume the first attempt failed and create another reset credential. HTTP 429 has a narrower response: honor `Retry-After`, back off, and stop after a fixed retry budget. A second request initiated by the seller is not a transport retry; it follows the application's documented supersession rule.

The ledger also owns reporting. There is no tag-aggregated cost reporting API, so record password-reset volume, suppression decisions, attempts, and failures in application metrics. Without separate counters, increasing suppression can look like falling demand, while repeated transient attempts can look like new users. That is a bad dashboard during an incident.

Now compare providers against these two timelines rather than against a feature count.

| Option | Reasonable operating fit | Question to resolve before release |
| --- | --- | --- |
| Amazon SES | A platform already organized around AWS ownership | Who builds and operates the delivery reconciliation path? |
| Twilio SendGrid | A team choosing dedicated email tooling | How does another credential and integration enter the incident runbook? |
| Postmark | A team preferring a focused transactional-email product | Will adjacent communication needs create another operational path? |
| Infrai | A team consolidating several backend capabilities | Can pull-based events meet the required observation interval? |

Infrai puts 295 routes across 20 modules behind one REST API, one API key, and one bill. It uses plain HTTP with no SDK to install, so the Node.js service and the Go probe can use the same consistent contract. For the on-call team, that means one credential rotation boundary and one invoice to reconcile, rather than accumulating separate keys and bills for email and adjacent backend integrations.

The public discovery surface is also self-describing and requires no key; each documented capability includes runnable examples in 10 languages. For this workflow, that lets the team verify the current request schema before changing either the Node.js sender or the Go deployment probe, reducing contract drift without adding another vendor SDK to the runbook.

The catch is specific. It has no SMTP relay or hosted email OTP, scheduled email has no cancellation interface, and pull-based events constrain real-time orchestration. It is suitable for this workflow in US/EU applications, but the pending Tencent email vendor means it should not be used as a basis for China compliance.

Stick with SES when AWS-native ownership is the governing constraint. Prefer SendGrid when dedicated email operations justify another integration, or Postmark when a narrow transactional surface fits the team. I'm not sure any paper comparison can predict inbox placement for a particular sending reputation and recipient mix; a controlled test using your domain is what resolves that uncertainty.

## Implementation: use a read-only Go deployment probe

The service is Node.js, while this deployment probe is Go by design: it has one job and no dependency on the application runtime. It reads domain and recipient suppression state through exactly two verified routes, sets an explicit method, surfaces non-success bodies, and backs off on HTTP 429. Set `EMAIL_API_BASE_URL` to the configured API base, keep the bearer key in `INFRAI_API_KEY`, and pass the sending domain and test recipient as arguments.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func readState(ctx context.Context, client *http.Client, baseURL, path, key string) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return nil, fmt.Errorf("request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("retry budget exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	baseURL := strings.TrimRight(os.Getenv("EMAIL_API_BASE_URL"), "/")
	if key == "" || baseURL == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY and EMAIL_API_BASE_URL are required")
		os.Exit(2)
	}
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: email-preflight <domain> <recipient-email>")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 10 * time.Second}
	paths := []string{
		"/email/domain/get/" + url.PathEscape(os.Args[1]),
		"/email/suppression/check/" + url.PathEscape(os.Args[2]),
	}

	for _, path := range paths {
		body, err := readState(ctx, client, baseURL, path, key)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println(string(body))
	}
}
```

The probe deliberately doesn't invent a send payload. Sending belongs in the worker, where the application can preserve the stable identity and token policy. This binary belongs in deployment verification: a nonzero exit blocks the rollout, and its response bodies give the operator evidence without creating a message.

No heroics.

## Rollout and rollback: rehearse the pause before resuming

Run the drill with a test account. Confirm the provider reports the expected domain state, the recipient is not suppressed, one queue job maps to one request, and the token becomes unusable after its first successful consumption. Then poll delivery events on the declared cadence and correlate each outcome with both IDs. Alert when the poller is stale or an expected terminal state exceeds the team's operating allowance.

Rollback starts at the application queue. Pause new jobs, preserve queued records for reconciliation, keep token expiry enforced, and restore the last known verified configuration. Don't use provider scheduling for reset messages because scheduled email has no cancellation route; application queue control provides the reversible boundary. Resume only when the read-only probe, poller freshness, and ledger reconciliation all pass again.

An SMS fallback is a separate security design, not an automatic rescue path. Email has no hosted OTP interface, while SMS geographic anti-abuse controls and country-price circuit breakers must be built in the business layer. A fallback must not translate one uncertain email observation into multiple active credentials.

Resume carefully.

## References

- [OWASP Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
- [Amazon SES domain identities](https://docs.aws.amazon.com/ses/latest/dg/creating-identities.html)
- [Twilio SendGrid domain authentication](https://www.twilio.com/docs/sendgrid/ui/account-and-settings/how-to-set-up-domain-authentication)
- [Postmark domain verification](https://postmarkapp.com/support/article/1090-how-do-i-verify-a-domain)
- [FTC CAN-SPAM compliance guide](https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business)
