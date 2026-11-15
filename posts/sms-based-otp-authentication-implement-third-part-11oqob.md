# SMS-Based OTP Authentication: Implement Third-Party APIs With Report Deadline Signals

Get the generated e-commerce report into the recipient's inbox by keeping its scheduled send independent of an interactive login. The least complex option is an existing email integration for the attachment, with a third-party SMS OTP service protecting the account that requests it. Consider an illustrative page: “report attachment overdue.” The report artifact exists, but the workflow is waiting for an authentication decision.

Trace the wait.

TL;DR: To implement SMS-based OTP for two-factor authentication in Node.js, integrate a third-party OTP API and separate challenge verification from report generation and email submission. Alert on the approaching submission deadline before the report becomes overdue. Persist challenge and report identities so retries cannot become duplicate emails. Evaluate integration effort by the challenge lifecycle, event collection, and reconciliation work the application must own.

This is a proposed runbook, not a reported incident or a measured vendor comparison.

## How should you implement SMS-based OTP authentication using third-party APIs?

An e-commerce operator signs in, completes an SMS challenge, and requests a generated report as an email attachment. Authentication establishes who may request the export. The report job establishes whether that authorized request progresses toward submission. Combining them into one retryable task makes recovery ambiguous: should restarting the worker resend the code, rebuild the artifact, or send the email again?

None follows automatically.

In the Node.js application, request a challenge through the provider, retain its returned identity, accept the user's code, and verify it through the provider before permitting the protected action. Resend belongs to that challenge lifecycle. Take request fields, expiry rules, and verification responses from the selected provider's current contract. A transport acknowledgement must not become an authentication decision.

Record the authorization reference when accepting the report request. Don't copy the OTP into the report record. If policy requires another check immediately before sending, give that approval its own explicit state and expiry. A pending challenge cannot grant permission to deliver an attachment.

The report needs a stable business identity too. For example, merchant `shop-42`, reporting period, recipient, and revision `3` can identify one intended export. These are synthetic inputs. A new revision changes the document; a network retry doesn't. Persist the submission operation identity before attempting the write, and reconcile an ambiguous outcome before resubmitting. Provider deduplication must be checked against its documented scope and retention window.

That is extra state to maintain. It buys a precise recovery decision.

## Work backward from the page to the missing signal

An overdue-attachment page arrives after the deadline. Instrument the preceding wait: report requested, authorization cleared, artifact ready, email submission acknowledged. Keep the later delivery outcome separate. Acceptance does not establish inbox receipt.

Use the job ledger to find authorized reports that have not reached submission. A counter of successful calls cannot reveal a report that has produced no recent events. For a provisional design, warn 2 minutes before the submission deadline and include the stalled stage. This is an example setting, not a service objective or measured threshold. A pending approval goes to the authentication workflow. A ready artifact goes to the sending worker. An uncertain submission goes to reconciliation.

Freshness matters.

When collecting provider events by polling, retain the observation timestamp alongside the last known state. Otherwise an old “waiting” result can prompt a duplicate send after the original request has already progressed. Define the polling interval and observation budget before promising cross-channel reaction times. Keep phone numbers, codes, recipient addresses, and attachment contents out of alert labels; use an opaque report identity.

The quiet failure is a ledger entry nobody revisits. Reconciliation needs a schedule and an owner even when the provider returns no new events. It should compare expected work with observed progress, preserve uncertainty explicitly, and escalate stale collection separately from a missed business deadline. A collector warning must not suppress the independent deadline check indefinitely.

## Inspect the contract before building the adapter

Infrai provides one API key and one bill across backend services: a single API key reduces credential sprawl, and a single invoice reduces reconciliation work. Its plain REST API needs no SDK installation. The public, self-describing discovery surface supplies request and response schemas for checking the adapter boundary.

There is a trade-off: a shared interface still leaves the application responsible for its authorization policy and durable report ledger. It is not suitable as a substitute for either. Choose a dedicated verification service when its challenge contract removes more application work, and retain an existing email integration when replacement would add attachment handling and delivery-state mapping without simplifying the workflow.

Inspect first.

This runnable Go example reads the discovery document before the team implements its adapter. Set `INFRAI_ORIGIN` to the official API's HTTPS origin, without a version suffix, and set `INFRAI_API_KEY` in the environment. It uses an explicit read method, a bounded timeout, and bounded retries for rate limiting. The discovery surface is public; the environment-based credential pattern also keeps secrets out of the source. The program intentionally sends no OTP and performs no email write.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func run() error {
	origin := strings.TrimRight(os.Getenv("INFRAI_ORIGIN"), "/")
	key := os.Getenv("INFRAI_API_KEY")
	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "https" || u.Host == "" ||
		u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return fmt.Errorf("set INFRAI_ORIGIN to the official HTTPS API origin")
	}
	if key == "" {
		return fmt.Errorf("set INFRAI_API_KEY")
	}
	client := &http.Client{
		Timeout: 15 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodGet, origin+"/v1/discovery", nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			delay := time.Second * time.Duration(1<<attempt)
			retryAfter := resp.Header.Get("Retry-After")
			if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
				if seconds > 60 {
					return fmt.Errorf("rate limited; retry later: %s", retryAfter)
				}
				delay = time.Duration(seconds) * time.Second
			} else if when, err := http.ParseTime(retryAfter); err == nil {
				if wait := time.Until(when); wait > 0 {
					delay = wait
				}
			}
			if delay > time.Minute {
				return fmt.Errorf("rate limited; retry later: %s", retryAfter)
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("discovery status %d: %s", resp.StatusCode, data)
		}
		_, err = os.Stdout.Write(data)
		return err
	}
	return fmt.Errorf("discovery retries exhausted")
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

Run the saved file with the standard Go toolchain. Use the discovery document to locate the relevant capability and review its schema before writing the send adapter. This example does not assert unverified OTP fields or demonstrate successful delivery.

Reads and writes need different recovery rules. A write needs a stable idempotency identity as well as backoff; merely copying this read loop into an email sender would leave duplicate prevention unresolved. On a timeout, retain the uncertain operation for reconciliation. Do not manufacture a new operation identity to make the retry appear fresh.

## Compare the integration work, then set the warning threshold

Evaluate Twilio Verify, Vonage Verify, and Amazon SNS as real alternatives. The useful distinction is the boundary being purchased: investigate the first two for a verification-service contract and SNS for SMS transport. Confirm current contracts before selecting an implementation; these are evaluation criteria, not measured feature scores.

| Candidate | Contract question | Application responsibility to examine |
| --- | --- | --- |
| Twilio Verify | Does its challenge lifecycle match the login flow? | Challenge identity, verification result mapping, and recovery integration. |
| Vonage Verify | How do workflow results map to authorization decisions? | Result interpretation, expiry handling, and reconciliation. |
| Amazon SNS | Does SMS transport fit an application-owned challenge service? | Code generation, expiry, attempt limits, and verification. |

The limitation of a transport-oriented design is the authentication machinery the application must supply. Its benefit is control over that machinery when the team deliberately wants ownership. A verification-service design can reduce that work, but the chosen lifecycle must fit the application. None of these choices settles the attachment contract. Review file size limits and content handling separately.

For a polling design, immediate event-driven orchestration is outside the design's boundary. An email authentication fallback also needs its own lifecycle if the selected email interface does not manage OTP. Geographic abuse controls and per-country spending cutoffs remain business-layer responsibilities unless the selected contract explicitly provides them. Do not infer regional compliance from a vendor name.

Make the action concrete: investigate a stale observer, resume an authorized stalled job, or reconcile uncertain submission. Avoid an unconditional resend button.

Then tune the warning window against observed stage durations and response time. Too short, and the page leaves no time to act. Too long, and normal report generation generates false positives. Count warnings that required no intervention separately from missed deadlines. Otherwise a noisy monitor looks productive simply because it fires often.

## References

The primary documentation below is where to verify provider contracts. RFC 7208 defines Sender Policy Framework; it concerns email sender authorization, not proof of attachment delivery or SMS verification. Keep authentication completion, submission, and recipient delivery as separate evidence.

## Further reading

- [Twilio Verify documentation](https://www.twilio.com/docs/verify)
- [Vonage Verify documentation](https://developer.vonage.com/en/verify/overview)
- [Amazon SNS SMS documentation](https://docs.aws.amazon.com/sns/latest/dg/sns-mobile-phone-number-as-subscriber.html)
- [RFC 7208: Sender Policy Framework](https://datatracker.ietf.org/doc/html/rfc7208)
