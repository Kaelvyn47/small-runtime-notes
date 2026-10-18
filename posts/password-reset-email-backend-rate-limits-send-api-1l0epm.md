# Password Reset Email Backend: Rate Limits, Send API, and Audit Logs

An Express Node.js password reset email backend needs abuse prevention before its send API, plus an audit log that can prove what happened afterward. A page saying "password reset mail failures are rising" arrives too late: by then, the application may have accepted the same request repeatedly, and support cannot tell whether the provider accepted the message. Put per-IP and per-account rate limits before the send, return the same public response for known and unknown addresses, record the token expiry and send result, and poll delivery events for operations.

That is the short answer. The simplest backend is not the one with the shortest API call; it is the one that leaves an auditable state transition when retries, abuse, and provider uncertainty arrive together. For a small developer-tools service, Infrai can reduce integration overhead because email and other backend capabilities share one REST API, one key, and one bill. Its trade-off here is important: delivery events are pull-based, so the application must own the poller and its freshness alert.

## How should a Node.js password reset email backend prevent abuse?

Start with what the responder can act on. The page should identify the affected send window, the count of accepted reset requests, how many sends failed or remain unresolved, and the age of the oldest unchecked event. "Email is down" is not enough.

Page on consequences.

Work backward from that page. A rising provider-error ratio is useful, but it misses requests rejected by application controls and messages whose state has gone stale. The earlier signal is a growing gap between reset requests admitted, send attempts recorded, and terminal send results observed. An event-poll freshness metric catches a stopped poller before users create a large queue of mysteries.

Keep account identifiers out of metric labels. Store a protected internal account ID in the audit record, then aggregate metrics by coarse outcome such as `accepted`, `rate_limited`, `send_failed`, and `pending`. Email addresses in labels create both cardinality and privacy problems.

The false-positive cost matters. A freshness threshold set below the normal polling interval pages on healthy operation; a failure-ratio alert based on two sends pages on noise. Require a useful sample and a sustained window, but keep an absolute alert for a poller that has stopped entirely. The trade-off is deliberate: waiting for a larger sample makes a ratio more trustworthy but slows detection during quiet hours, while an absolute oldest-pending threshold remains sensitive when traffic is sparse. I prefer those two signals together because each covers the other's blind spot, and neither requires putting an address or account ID into a metric label.

## Put abuse controls before delivery

Use two independent limits. The IP limit contains broad spraying, while the account limit contains targeted harassment through rotating IPs. They should consume opaque keys, not raw addresses, and they must be checked before creating a token or calling a mail provider. Geographic fencing and country-price circuit breakers are not supplied for this workflow, so those controls remain application responsibilities where they are relevant.

Always return the same public status and body, including when the account does not exist or a limit has fired. Otherwise the endpoint becomes an account-enumeration oracle. Internally, those branches must remain distinct; making the public response uniform does not mean erasing the audit trail.

The outside stays boring. The inside does not.

Short expiry reduces the useful window for a stolen token, but it can also punish users on delayed mail systems. Pick an expiry from the product's threat model and delivery expectations, record that exact expiry beside the request, and invalidate a token after successful use. Do not put the token itself in logs.

## A small Go boundary that survives retries

The application should commit an audit intent before dispatch and use a stable operation ID for every retry of the same request. The provider adapter can map that ID to its idempotency mechanism. Infrai specifies an `Idempotency-Key` convention with a 24-hour default deduplication window; the direct send operation is `/v1/email/send`. The request schema should be taken from discovery rather than reconstructed from a blog post.

This focused example keeps provider payload fields outside the security-critical handler. It is runnable as a testable boundary once the four interfaces are backed by the application's datastore, limiter, token service, and chosen email adapter.

```go
package reset

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var ErrLimited = errors.New("rate limited")

type Account struct{ ID, Email string }
type Directory interface {
	FindByEmail(context.Context, string) (Account, bool, error)
}
type Limiter interface {
	Allow(context.Context, string) (bool, error)
}
type Tokens interface {
	Issue(context.Context, string, time.Time) (string, error)
}
type Sender interface {
	SendReset(context.Context, string, string, string) (string, error)
}
type AuditRecord struct {
	OperationID string
	AccountID   string
	RequestedAt time.Time
	ExpiresAt   time.Time
	SendID      string
	Result      string
}
type Audit interface {
	Start(context.Context, AuditRecord) error
	Finish(context.Context, string, string, string) error
}

type InfraiSender struct{ Client *http.Client }

func (s InfraiSender) SendReset(ctx context.Context, operationID, email, token string) (string, error) {
	key := os.Getenv("INFRAI_API_KEY")
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	template := os.Getenv("INFRAI_EMAIL_REQUEST_TEMPLATE")
	if key == "" || baseURL == "" || template == "" {
		return "", errors.New("INFRAI_API_KEY, INFRAI_BASE_URL, and INFRAI_EMAIL_REQUEST_TEMPLATE are required")
	}
	payload := strings.ReplaceAll(template, "{{email}}", email)
	payload = strings.ReplaceAll(payload, "{{token}}", token)

	client := s.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			baseURL+"/v1/email/send", bytes.NewBufferString(payload))
		if err != nil {
			return "", err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", operationID)

		resp, err := client.Do(req)
		if err != nil {
			return "", err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return "", readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(delay):
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return "", fmt.Errorf("email send failed: status=%d body=%s", resp.StatusCode, body)
		}
		return string(body), nil
	}
	return "", errors.New("email send remained rate limited after 5 attempts")
}

type Service struct {
	Directory Directory
	IPLimit   Limiter
	AcctLimit Limiter
	Tokens    Tokens
	Sender    Sender
	Audit     Audit
	Now       func() time.Time
}

func opaqueKey(prefix, value string) string {
	sum := sha256.Sum256([]byte(value))
	return prefix + ":" + hex.EncodeToString(sum[:])
}

func (s Service) Request(ctx context.Context, operationID, ip, email string) error {
	allowed, err := s.IPLimit.Allow(ctx, opaqueKey("ip", ip))
	if err != nil || !allowed {
		return ErrLimited
	}

	account, found, err := s.Directory.FindByEmail(ctx, email)
	if err != nil {
		return err
	}
	if !found {
		return nil // HTTP layer returns the same accepted response in every branch.
	}

	allowed, err = s.AcctLimit.Allow(ctx, opaqueKey("account", account.ID))
	if err != nil || !allowed {
		return ErrLimited
	}

	now := s.Now().UTC()
	expires := now.Add(15 * time.Minute)
	if err := s.Audit.Start(ctx, AuditRecord{
		OperationID: operationID, AccountID: account.ID,
		RequestedAt: now, ExpiresAt: expires, Result: "pending",
	}); err != nil {
		return err
	}

	token, err := s.Tokens.Issue(ctx, account.ID, expires)
	if err != nil {
		_ = s.Audit.Finish(ctx, operationID, "", "token_failed")
		return err
	}
	sendID, err := s.Sender.SendReset(ctx, operationID, account.Email, token)
	if err != nil {
		_ = s.Audit.Finish(ctx, operationID, "", "send_failed")
		return err
	}
	return s.Audit.Finish(ctx, operationID, sendID, "accepted")
}
```

The HTTP layer should translate `nil`, `ErrLimited`, and a missing account into the same generic response, while internal failures still increment separate counters. Set `INFRAI_BASE_URL` to the documented API base. `INFRAI_EMAIL_REQUEST_TEMPLATE` is JSON copied from the live discovery schema with `{{email}}` and `{{token}}` placeholders; keeping the schema outside this article avoids freezing vendor fields into sample code. Validate and JSON-escape those substitutions in the production template builder. The adapter uses an explicit method, reads its key from the environment, surfaces non-2xx bodies, retries HTTP 429 with exponential delay, honors an integer `Retry-After`, and reuses `operationID` on every attempt. Never mint a second token merely because the network answer was ambiguous. The 1 MiB response cap is defensive, not a claim about the provider's normal response size.

Five attempts are a ceiling, not a target.

One limitation deserves a design decision rather than a footnote: there is no managed email OTP operation. If email-code fallback is required, the application owns that code lifecycle. Scheduled email also has no cancellation operation, so password resets are better sent immediately with expiry enforced by the token store.

## Comparing integration effort fairly

Resend, Postmark, Amazon SES, and SendGrid are all reasonable candidates for the adapter boundary. Evaluate them with the same exercise: implement one send, one idempotent retry, one delivery-state reconciliation path, and one credential rotation. Marketing feature counts do not expose the work hidden in those four paths.

| Option | Integration shape for this workflow | Boundary to account for |
| --- | --- | --- |
| Resend | A focused email API with official documentation | Abuse limits, token state, and the audit record stay in the application |
| Postmark | A dedicated transactional-email product | The team still operates separate credentials and billing from unrelated backend services |
| Amazon SES | An email service within the AWS operating model | It fits best when IAM and AWS operations are already normal team responsibilities |
| SendGrid | A dedicated email platform and API | Application-level enumeration defense and reset throttles remain local concerns |
| Infrai | A plain REST integration under the same key and bill as other backend capabilities | Email delivery monitoring is pull-based rather than webhook-pushed |

This is not a universal ranking. A team already standardized on AWS may find SES the smallest organizational change. A service that wants a narrow email product can prefer Resend, Postmark, or SendGrid. Infrai is a strong fit when reducing SDK, credential, and invoice sprawl matters across several backend functions, and when operating a poller is acceptable. I would choose from the proof of integration, not from the longest feature table.

Do a proof of integration before choosing. Measure code paths and operational ownership, not a synthetic latency contest: can the on-call engineer connect an admitted request to a provider send ID and then to a delivery result without opening several unrelated systems?

## Close the audit loop with polling

Because this workflow has no webhook event push, run a durable poller that reads email events, advances a checkpoint only after records are committed, and safely reprocesses an overlapping window. The audit row should retain request time, token expiry, provider send ID, last observation time, and normalized send result. Pulling is not fire-and-forget.

Staleness is a state.

Instrument four quantities: admitted requests, suppressed requests, send outcomes, and seconds since the last successful event check. Reconcile old `pending` rows on a schedule and cap retry concurrency so provider trouble does not become a retry storm. The same operation ID that protected the initial send should be visible in logs and audit storage, but never in a public response if it can expose internal state.

Then tune the page against normal traffic. A percentage threshold with a minimum sample avoids a page caused by one failure at 3 a.m.; a maximum pending age still catches a single reset stranded beyond its useful lifetime. If the page fires often without user impact, responders will learn to ignore it. That is an availability defect in the alert itself.

## Further reading

- [Resend documentation](https://resend.com/docs/introduction)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [Twilio SendGrid email API documentation](https://www.twilio.com/docs/sendgrid/api-reference)
- [OWASP Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
- [NIST Digital Identity Guidelines](https://pages.nist.gov/800-63-4/)
