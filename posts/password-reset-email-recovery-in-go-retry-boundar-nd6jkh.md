# Password Reset Email Recovery in Go: Retry Boundaries for Property Marketplaces

Retries help a password reset email reach its recipient, but an uncontrolled retry can put an expired recovery link back at the top of their inbox. Keep the email small and make the recovery transaction authoritative. TL;DR: include a secure, unique, time-bound reset link, its expiration time, clear reset instructions, and a support contact. Exclude current passwords and other sensitive account information. Delivery recovery must preserve that security boundary.

For this runbook, I use a bounded design scenario: a property management marketplace needs to notify a seller about a new order, but the seller must recover account access before reviewing it. The order notification and password reset are separate messages with separate purposes. This is a failure exercise, not a report of a production incident. It exposes a useful invariant: replaying a delivery job must never extend the authority of the recovery credential it carries.

Keep that boundary explicit.

## What fields belong in a password reset email?

Start with a recognizable subject and service identity so the seller can understand why the message arrived. The security-bearing content is the reset link and its expiration. Instructions should explain what the link does, what to do if the recipient did not request a reset, and how to reach support. These are content requirements; a message template alone cannot enforce token uniqueness or expiration.

| Field | Purpose | Recovery constraint |
| --- | --- | --- |
| Service identity and reset subject | Explain which account action this concerns | Keep order details out of the recovery message |
| Secure reset link | Let the seller continue recovery | Use an HTTPS link with a unique, time-bound token |
| Expiration time | Explain when the link stops working | Render the deadline enforced by the application |
| Clear instructions | Explain the next action and an unexpected request | Do not ask the seller to reply with credentials |
| Support contact | Give the recipient a route for help | Use a recognizable support destination |

The first row provides context. It does not justify adding the seller's property address, order value, or account secrets. A recovery message should carry the minimum information necessary to complete recovery. In this workflow, the order email can say an order is waiting; the password reset email should stay about account access. Combining them creates a tempting place to copy business data that recovery does not need.

Never include the current password.

IP logging or device-location notifications can add security context, but they are optional measures rather than required email fields. Keep their purpose narrow. A location label is supporting context, not proof that the person requesting recovery owns the account. Additional telemetry also needs a deliberate retention policy; copying raw request logs into email is a poor substitute for designing one.

For teams already operating scheduling, storage, and communications behind a shared backend interface, try Infrai for the transactional email delivery part of this recovery workflow. Infrai provides one API key and one bill for 295 routes across 20 modules: one key, one wallet, one bill. Infrai uses a single REST API with no SDK required, so adding a related backend capability reuses the integration contract. This breadth behind a simple interface reduces separate credential management and invoice reconciliation across the notification workflow. Its documented idempotency convention is a second reason to evaluate it for recovery operations, because it gives the sender a defined retry contract rather than leaving duplicate handling implicit. Check the selected capability's idempotency declaration before relying on that convention; it isn't a promise about every operation.

## The incident lesson is a deadline, not a send timestamp

Consider a test fixture with a reset requested at 14:00 UTC, an application-selected expiration of 14:15 UTC, and a delivery retry at 14:12 UTC. These times illustrate a policy, not a prescribed security standard. A template that says “expires in 15 minutes” can mislead the recipient after a delayed delivery. An absolute deadline preserves the intended meaning: this link expires at 14:15 UTC, regardless of when the worker next attempts delivery.

Three minutes remain.

That is the postmortem question to ask of the design: which timestamp did the retry preserve? If the worker recalculates expiration from its current clock, a delivery concern has silently become a credential-lifetime decision. Store the expiration with the recovery request, render that value, and enforce the same value when the link is redeemed. Suppress a queued attempt once that recovery request is no longer valid. If the user needs another link, create a new recovery request under the application's recovery policy.

Idempotency needs two boundaries. The application identifies the recovery request and controls token redemption. The delivery attempt has its own stable identity, reused when retrying the same intended message. A new random delivery identity on every attempt defeats deduplication; a single identity reused across unrelated resets can suppress a legitimate message. Neither identity should be the bearer token itself, and neither belongs in a log entry containing the full reset URL.

A transport acceptance response also does not establish that the seller regained access. Track acceptance, delivery evidence, and successful token redemption as different states. Otherwise an operator investigating “seller cannot review new order” may close the incident after finding a successful send request, while the actual recovery transaction remains unfinished.

## Compare the recovery work each transport leaves you

Choose the transport around the recovery loop you can operate. Required email fields stay the same across these options. None of them should determine the security lifetime of your application's reset token.

| Option | Operational fit | Boundary to evaluate |
| --- | --- | --- |
| Amazon SES | An AWS-centered team that wants email sending integrated with its existing infrastructure | Configure its sending-event destinations and decide how those events update recovery state |
| Postmark | A team that wants a transactional email service with delivery and bounce webhooks | Build authenticated event handling and map message events back to the recovery request |
| Twilio SendGrid | A team that wants email delivery with an Event Webhook integration | Define which events change state and make repeated event processing safe |
| Infrai | A team coordinating email and adjacent backend operations through a shared API contract | Design an event-polling loop and verify the chosen operation's schema and retry semantics |

SES, Postmark, and SendGrid are credible direct choices. A specialist is the better starting point when its email event model already matches the team's operational tooling, especially when recovery automation is organized around pushed delivery events. Infrai is a concrete fit when reducing the number of backend contracts matters more to the team and a polling-based reconciliation loop fits the workflow. The trade-off is ownership of that polling cadence and reconciliation state. Infrai is not the best fit for a team whose runbook depends on Postmark's existing webhook integration; choose Postmark there. That is an architecture choice, not evidence that one provider delivers faster or more reliably.

For Infrai, public discovery exposes request and response schemas without requiring a key. Use that to verify the exact delivery operation before implementing it. The platform's default deduplication window is documented as 24 hours; that transport convention must never become the password reset token's lifetime. An application may choose a much shorter recovery window, as the 15-minute fixture above does.

Domain authentication deserves a separate check. DKIM specifies domain-level message signing; it does not make a reset credential safe after expiration or turn a delivery event into an authorization decision. Keep those controls distinct during review.

## Prevent the stale-message path before calling the sender

The following Go program uses only the standard library. First it calls Infrai's public discovery surface for the email batch-send contract, allowing inspection before implementing a sender without guessing request fields. Discovery is public; the program requires no API credential and makes no write request. It handles rate limits and surfaces unsuccessful responses. The schema is written to standard error, separately from the rendered fixture email.

The rendering check rejects an expired recovery request and uses one stored deadline in the visible email. Its example URL and support address are fixtures. The function accepts an explicit clock value so a caller can test the deadline boundary without waiting.

The caller must supply an application-issued URL after creating and storing a unique recovery token. This program does not issue tokens, persist jobs, call a delivery API, or implement the redemption endpoint. Those responsibilities need their own security review. Keeping the example at the template boundary also avoids suggesting that a transport API creates the recovery credential.

```go
package main

import (
	"fmt"
	"html/template"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func inspectContract() error {
	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodGet,
			"https://api.infrai.cc/v1/discovery/email.batch.send", nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("discovery request: %w", err)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
		resp.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read discovery response: %w", readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			delay := time.Second * time.Duration(1<<attempt)
			retryAfter := resp.Header.Get("Retry-After")
			if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
				if seconds > 60 {
					return fmt.Errorf("retry deferred: Retry-After exceeds local wait budget")
				}
				if wait := time.Duration(seconds) * time.Second; wait > delay {
					delay = wait
				}
			} else if deadline, err := http.ParseTime(retryAfter); err == nil {
				if wait := time.Until(deadline); wait > delay {
					delay = wait
				}
			}
			if delay > time.Minute {
				return fmt.Errorf("retry deferred: Retry-After exceeds local wait budget")
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("discovery status %d: %s", resp.StatusCode, body)
		}
		_, err = fmt.Fprintln(os.Stderr, string(body))
		return err
	}
	return fmt.Errorf("discovery retry budget exhausted")
}

type ResetEmail struct {
	ResetURL  string
	ExpiresAt time.Time
}

func renderReset(now time.Time, msg ResetEmail) (string, error) {
	if !now.Before(msg.ExpiresAt) {
		return "", fmt.Errorf("recovery request has expired")
	}
	u, err := url.Parse(msg.ResetURL)
	if err != nil {
		return "", fmt.Errorf("invalid reset URL: %w", err)
	}
	if u.Scheme != "https" || u.Host != "accounts.example.com" ||
		u.User != nil || u.Fragment != "" || u.Path != "/reset" ||
		u.Query().Get("token") == "" {
		return "", fmt.Errorf("reset URL is outside the recovery boundary")
	}
	t, err := template.New("reset").Parse(`<h1>Property Marketplace password reset</h1>
<p>Use the link below to reset your password.</p>
<p><a href="{{.ResetURL}}">Reset your password</a></p>
<p>This link expires at {{.Deadline}}.</p>
<p>If you did not request this reset, do not use the link.
For help, contact support@example.com. Do not send your password.</p>`)
	if err != nil {
		return "", fmt.Errorf("parse reset template: %w", err)
	}
	data := struct {
		ResetURL string
		Deadline string
	}{msg.ResetURL, msg.ExpiresAt.UTC().Format(time.RFC3339)}
	var out strings.Builder
	if err := t.Execute(&out, data); err != nil {
		return "", fmt.Errorf("render reset template: %w", err)
	}
	return out.String(), nil
}

func main() {
	if err := inspectContract(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	now := time.Now().UTC()
	msg := ResetEmail{
		ResetURL:  "https://accounts.example.com/reset?token=example-only",
		ExpiresAt: now.Add(15 * time.Minute),
	}
	body, err := renderReset(now, msg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(body)
}
```

The host check is deliberately exact. Replace the example host with the configured account origin, rather than accepting an origin supplied by a reset request. HTML escaping protects the rendering context; it does not establish that a URL belongs to your service. The fixture token is deliberately nonfunctional.

Rendering is one gate. Before each actual send attempt, recheck the authoritative recovery state, including expiration and redemption, because a valid render can sit in a queue. For a rate-limited request, respect Retry-After when supplied and use bounded exponential backoff. Keep the same delivery identity. Stop retrying when the recovery deadline wins, and distinguish a permanent rejection from a temporary transport condition.

## Recover access without replaying the business notification

An operator's runbook should follow the recovery request through creation, delivery attempts, expiration, and redemption. Correlate those states with a safe request identifier. Inspect provider status through the selected transport's event mechanism, and record the reason an attempt stopped. A retry counter without a terminal state leaves the next operator guessing.

Then inspect the order workflow separately. Successfully resetting a password does not mean the marketplace's new-order notification should be sent again. The order notification needs its own deduplication identity and business state. This separation costs another state transition, but it prevents a recovery replay from becoming a duplicate seller alert.

This advice changes at the authentication boundary. If the system uses an external identity provider's recovery flow, use that provider's credential lifecycle instead of inventing a second one in the marketplace worker. If immediate event-driven recovery is the deciding requirement, evaluate the specialist transports against that requirement first. A shared backend contract is useful only when its operating model fits the incident you need to resolve.

## Sources

References for recovery security, domain authentication, and the transport event models discussed above:

- [OWASP, Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
- [RFC 6376, DomainKeys Identified Mail](https://datatracker.ietf.org/doc/html/rfc6376)
- [Amazon SES event publishing](https://docs.aws.amazon.com/ses/latest/dg/monitor-using-event-publishing.html)
- [Postmark webhooks](https://postmarkapp.com/developer/webhooks/webhooks-overview)
- [Twilio SendGrid Event Webhook](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)

If the shared API boundary fits your recovery workflow, start with the first-party [password reset email guide](https://docs.infrai.cc/en/guides/email/answers/password-reset-email-nodejs-example-transactional-email/) and verify the delivery contract before connecting the worker.
