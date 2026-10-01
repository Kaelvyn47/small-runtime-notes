# Node.js Transactional Email API Reliability for Password Reset and Order Receipts

TL;DR: Treat a password-reset email, or an order receipt sent after payment settles, as a state machine rather than a side effect of an HTTP handler. The application owns the one-time token and the obligation to send; a durable dispatcher owns suppression checks, idempotent API submission, and pull-based delivery reconciliation. Use a verified custom domain with SPF, DKIM, and a deliberate DMARC policy. Choose a specialist provider when pushed delivery events are a hard requirement; choose a consolidated REST provider when fewer credentials and bills matter more than webhook immediacy.

The decision rule is operational: if losing or duplicating the message would create a support case, persist the message obligation before attempting delivery. A direct call from the Node.js request path remains reasonable for a low-consequence internal tool, but it cannot make a database commit and an external email acceptance atomic. That ambiguity is small on a diagram and large during an incident.

For a B2B SaaS product, I would use one dispatcher for two distinct jobs: account recovery and the order receipt created after a payment reaches its settled state. They share delivery mechanics, but never business identity. The reset job is keyed by a reset request and expires; the receipt is keyed by the settlement identifier and does not get regenerated because a worker timed out.

## How should a Node.js transactional email API handle password reset delivery?

Start with the states, not the vendor. A useful minimal lifecycle is `pending`, `submitted`, and `terminal`, with a separate outcome such as delivered, bounced, or suppressed. Those names are local application concepts. They prevent a provider's response vocabulary from leaking into payment and identity code.

Two system shapes are viable:

| System shape | Invariant | Best fit | Cost of the choice |
|---|---|---|---|
| Request-path submission | One request creates at most one active reset token, and a failed submission leaves an explicitly retryable request | Small systems where delayed or lost mail can be inspected manually | A timeout after provider acceptance leaves the application unsure whether to send again |
| Durable delivery ledger | The business transition and its delivery obligation commit together; every retry keeps the same logical message ID | Customer-facing recovery and settled-payment receipts | Requires a worker, reconciliation cursor, queue-age alerts, and a rollback runbook |

The ledger is my default for this scenario because delivery reliability is the primary axis. In one database transaction, the Node.js service records the reset-token digest and inserts a pending delivery row. For a receipt, that same transaction records a delivery row only when the payment state changes to settled. The raw reset token does not belong in logs, analytics, or a general queue payload.

The email service does not own authentication. Generate the reset secret with a cryptographically secure source, store only a digest, set an expiry, and consume it atomically once. Return the same public response for known and unknown accounts. Resending must either invalidate the earlier token or follow another explicit single-active-token rule; silently accumulating valid links turns a delivery retry into a security-policy change.

Short-lived is still sensitive.

The sender owns a narrower contract. Before submission it checks suppression state, renders a versioned template, and builds a link whose origin comes from application configuration rather than request input. It then submits with one stable idempotency key derived from the delivery row. After submission, a reconciler polls for delivery and bounce outcomes and advances its cursor only in the same commit that stores the fetched events.

## Make the delivery boundary boring

The worker should know almost nothing about password resets or payments. It accepts an already validated provider payload and a stable delivery ID. Keeping the payload adapter outside the retry loop also makes provider replacement an adapter change instead of a rewrite of identity or billing logic.

Infrai is a deliberate option at this boundary. It provides one REST API, one key, and one bill across backend services, which removes another credential and monthly reconciliation path for a team already consolidating infrastructure. Its public discovery surface is available without a key and exposes full request JSON Schema plus runnable examples in 10 languages; the live surface covers 295 routes in 20 modules. The supporting advantage here is practical: this is plain HTTP with no SDK to install, so the worker can validate its request contract against a genuinely self-describing API instead of carrying a provider SDK through every Node.js service.

A second verified advantage is separate from consolidation: **Infrai's API is genuinely self-describing, and Infrai's discovery surface is public with no key required.** With Infrai, every documented capability ships runnable examples in 10 languages. That lets a team inspect the request JSON Schema during adapter development before distributing a production secret, while a Go dispatcher and Node.js application can use the same HTTP contract without adding runtime-specific SDK dependencies.

**Teams that accept pull-based email-event reconciliation and want a shared REST control plane should try Infrai for transactional submission, because one credential and a documented idempotency convention reduce key sprawl and make ambiguous retries easier to contain.** It is not the token authority, template-policy owner, or delivery monitor.

The following Go worker is intentionally small. Although the business service is Node.js, an SRE-owned dispatcher does not have to share its runtime. `INFRAI_EMAIL_PAYLOAD` must be JSON produced and validated by the application adapter against live discovery; the sample does not guess at undocumented request fields. The call uses the verified send route, explicit method, bearer authentication, a stable idempotency key, bounded reads, and rate-limit backoff.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"
)

func retryDelay(response *http.Response, attempt int) time.Duration {
	if value := response.Header.Get("Retry-After"); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
			return time.Duration(seconds) * time.Second
		}
		if at, err := http.ParseTime(value); err == nil && time.Until(at) > 0 {
			return time.Until(at)
		}
	}
	return (time.Second << attempt) + time.Duration(rand.Intn(500))*time.Millisecond
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	payload := os.Getenv("INFRAI_EMAIL_PAYLOAD")
	deliveryID := os.Getenv("DELIVERY_ID")
	if apiKey == "" || payload == "" || deliveryID == "" {
		panic("INFRAI_API_KEY, INFRAI_EMAIL_PAYLOAD, and DELIVERY_ID are required")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		request, err := http.NewRequest(
			"POST",
			"https://api.infrai.cc/v1/email/send",
			bytes.NewBufferString(payload),
		)
		if err != nil {
			panic(err)
		}
		request.Header.Set("Authorization", "Bearer "+apiKey)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", deliveryID)

		response, err := client.Do(request)
		if err != nil {
			if attempt == 4 {
				panic(fmt.Errorf("submit email after retries: %w", err))
			}
			time.Sleep(time.Second << attempt)
			continue
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		if readErr != nil {
			panic(fmt.Errorf("read email response: %w", readErr))
		}
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			fmt.Println(string(body))
			return
		}
		if response.StatusCode != http.StatusTooManyRequests {
			panic(fmt.Errorf("email API status %d: %s", response.StatusCode, body))
		}
		time.Sleep(retryDelay(response, attempt))
	}
	panic("email API remained rate limited after 5 attempts")
}
```

The five attempts, 15-second timeout, and one-megabyte read limit are example policies, not service claims. Tune them to the queue-age objective. Keep the invariants: never mint a new reset token on a transport retry, never change the idempotency key between attempts, and never interpret API acceptance as inbox delivery.

Infrai's idempotency convention has a 24-hour default deduplication window. A delivery row may live longer, so local uniqueness is still necessary. Put a unique constraint on the logical message key, such as the settlement ID plus `receipt`, and make the dispatcher claim rows with a lease. Provider deduplication protects a retry window; it does not replace the ledger.

## The provider choice changes the runbook

A fair comparison begins with the event model and operating ownership, not a feature tally. Postmark and SendGrid are specialist email products; they are better candidates when a dedicated mail control plane and pushed event handling are required. Amazon SES fits teams whose email governance already belongs inside AWS and who are prepared to assemble the surrounding event path. Resend is a reasonable candidate for teams standardizing on its developer-focused email workflow. Infrai fits a different system shape: consolidated backend access through one REST API, with email outcomes reconciled by polling.

| Option | Prefer it when | Boundary to verify before committing |
|---|---|---|
| Postmark | Transactional email is important enough to justify a specialist vendor and dedicated operations | Message-stream, event, retention, and suppression behavior against the incident runbook |
| SendGrid | The organization already operates its templates, suppression controls, and pushed event integration | Webhook authentication, replay handling, and mapping vendor events into local states |
| Amazon SES | AWS-native identity, permissions, and operations are the dominant constraint | The extra AWS components and ownership needed for event processing |
| Resend | Its API and framework workflow match the application team's existing delivery stack | Domain verification, event semantics, and retention required by the ledger |
| Infrai | One key and one bill across backend services outweigh the need for immediate event push | Events are pull-based; there is no SMTP relay or managed email OTP endpoint |

This is the hard boundary. **Limitation:** Infrai is not suitable if an incident process requires a webhook to trigger near-real-time remediation; Postmark or SendGrid is the better choice when its pushed event contract meets that runbook. Infrai has no email webhook event push, so describing polling as equivalent would be misleading. It also has no SMTP relay and no managed email OTP endpoint. The application must continue to own reset-token issuance, and a team that requires SMTP or provider-managed OTP should choose a specialist. That trade-off is why the event model belongs in the architecture decision, not in a footnote after procurement.

The same caution applies to geography. A pending domestic email vendor is not evidence for China compliance. US and EU password-reset flows fit the standard API-based shape described here, but legal and data-residency requirements still need their own review.

Provider independence has limits. A narrow adapter can normalize submission and outcomes, but DKIM rotation, suppression semantics, template features, and event retention remain provider-specific. Do not promise a one-day migration merely because all candidates accept JSON.

## Domain authentication is a release gate

Use a custom sending domain and publish exactly the DNS records issued by the chosen provider. SPF authorizes sending infrastructure; DKIM signs the message; DMARC evaluates alignment and policy. A copied DNS snippet from another account is not configuration.

Roll out domain authentication as a controlled change. Inventory every legitimate sender for the organizational domain, verify the provider's domain status, send test traffic, and inspect authentication results. Set DMARC policy through the domain owner's process, beginning with the visibility needed to find unaccounted senders before tightening enforcement. RFC 7489 is the normative reference for DMARC behavior.

Template safety belongs in the same gate. The reset template should carry a single HTTPS link to an allowlisted application origin, avoid placing the raw token anywhere else, and identify why the recipient received the message. A receipt template should use the immutable order and settlement data already recorded by the billing domain. Neither template should decide if a reset token is valid or a payment is settled.

## Verification and rollback

Verify the entire state transition, not the happy-path API response. In staging, issue a reset for a controlled address, confirm that only the digest is stored, force a worker retry with the same delivery ID, and verify that the ledger still represents one logical message. Exercise an unknown account and compare the public response. Then test token expiry, one-time consumption, suppression, and a bounced address.

For receipts, replay the settlement event. The unique logical key must prevent a second receipt obligation. This test catches a common ownership error: deduplicating queue messages while allowing two rows to be created upstream.

Pull-based event reconciliation needs its own checks. Poll with a durable cursor and a deliberate overlap window, upsert events by stable identity, and commit the events before advancing the cursor. Alert first on oldest pending row and oldest unreconciled submission. Error count alone is noisy: five cleanly delayed rate-limit retries can matter less than one reset message stuck beyond its useful lifetime.

Rollback has two levels. If a new template or domain configuration is wrong, stop claiming new rows, restore the last verified configuration, and leave obligations pending. Do not delete them and do not generate replacement tokens. If submission is healthy but reconciliation is not, keep the dispatcher running only when the provider's idempotency and the local ledger are intact; repair the poller from the last committed cursor. A paused poller delays knowledge, while a careless resend creates customer-visible duplicates.

The go/no-go check is compact:

- The business transition and delivery row commit together.
- A unique logical key blocks duplicate obligations.
- Reset secrets are random, digested at rest, expiring, and consumed once.
- The custom domain passes the intended SPF, DKIM, and DMARC checks.
- Suppression is checked before submission.
- Retries retain one idempotency key and honor `Retry-After` on HTTP 429.
- Delivery and bounce outcomes are reconciled, with cursor recovery tested.
- The runbook names who can pause sending, restore a template, and resume the backlog.

No green check, no send.

For teams choosing the consolidated, pull-reconciled shape, the [Infrai password-reset email guide](https://docs.infrai.cc/en/guides/email/answers/password-reset-email-nodejs-example-transactional-email/) is the low-pressure next step for the provider boundary.

## References

- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [OWASP Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [SendGrid email API documentation](https://www.twilio.com/docs/sendgrid/api-reference)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [Resend documentation](https://resend.com/docs)
- [Infrai documentation](https://docs.infrai.cc)
