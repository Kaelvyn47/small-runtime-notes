# Node.js Marketplace Reset Email: Bounce, Complaint, and Suppression List Reconciliation

A marketplace password-reset path should treat delivery feedback as account state, not as a report someone reads later. The practical trade-off is integration effort: accept bounce and complaint events when they are available, poll the provider's suppression data as a reconciliation loop, and check one local suppression table before every send. **The short answer:** keep request handling, message delivery, and feedback ingestion separate; make every transition idempotent; and fail closed when the latest suppression state is unknown.

I have operated cron and queue infrastructure where missed jobs and duplicate deliveries were paging events. The useful incident lesson is narrower than any vendor comparison: a successful API response says that a provider accepted a message, not that the mailbox received it. A poller that silently stops can therefore leave the application repeatedly attempting an address that has already produced adverse feedback. The invariant is simple: no reset attempt may bypass a durable, current eligibility decision.

## How should a Node.js app poll an email bounce and complaint suppression list?

A remote suppression list is an important boundary, but it is a poor dependency for the synchronous reset endpoint. Network latency and provider availability would sit directly in an authentication flow, and a delayed polling cycle could race with a new reset request. A local projection avoids that coupling. It also gives the on-call engineer one place to answer, "Why did we decline this send?"

Fail closed.

Use a small state machine rather than a boolean. At minimum, distinguish a hard bounce, a complaint, and a temporary delivery failure. Hard bounces and complaints should block later attempts under an explicit policy. Temporary failures need a bounded retry policy; treating every transient failure as permanent can lock a legitimate buyer out, while retrying forever creates load and damages sender reputation.

There is another boundary: the reset token itself. NIST SP 800-63B describes out-of-band secrets as valid for only one successful authentication and within a limited validity period. A short-lived reset link should likewise be single-use, stored in a form that does not expose the bearer secret if the database is read, and invalidated after success. Delivery suppression and token expiry solve different risks. Neither substitutes for the other.

## The reconciliation loop is the control plane

The request path should enqueue an intent containing an internal message ID, account ID, normalized recipient key, template version, and token expiry. It should not wait for SMTP delivery. A worker claims that intent, reads the local suppression state, rejects an expired job, and only then asks the delivery adapter to send. Keep the raw address out of routine logs; a stable keyed digest is enough for correlation in most operational views. Feedback then arrives through two paths. Push notifications minimize delay. Polling repairs gaps caused by a disabled subscription, an expired credential, a deployment error, or an event the consumer failed to commit. The poller's checkpoint and the feedback records must be committed in the same database transaction. Advancing a cursor before applying its page is the classic missed-event failure: the job appears healthy, its next run starts after the missing page, and the local eligibility decision remains stale with no failed task left to retry. Applying a page before advancing its cursor is acceptable only when event application is idempotent. I default to at-least-once processing because it matches what queues and recovery procedures can honestly provide. Exactly-once claims tend to disappear during a replay. Give every feedback record a provider-scoped event key, enforce uniqueness in storage, and make a repeated hard-bounce transition a no-op. If the upstream feed has no stable event ID, derive a deduplication key from documented immutable fields and retain the original payload for audit.

Replays happen.

The following Go sketch shows the preventative path even when the surrounding web application is Node.js. The language boundary is deliberate: the contract, transaction ordering, and failure semantics matter more than an SDK call.

```go
package feedback

import (
	"context"
	"errors"
	"time"
)

var ErrSuppressed = errors.New("recipient is suppressed")

type Store interface {
	WithTransaction(context.Context, func(Tx) error) error
	Eligibility(context.Context, string) (allowed bool, checkedAt time.Time, err error)
}

type Tx interface {
	InsertEventOnce(eventKey, recipientKey, kind string, occurredAt time.Time) (inserted bool, err error)
	Suppress(recipientKey, reason string, occurredAt time.Time) error
	SaveCursor(source, cursor string) error
}

type Event struct {
	Key, RecipientKey, Kind string
	OccurredAt              time.Time
}

func ApplyPage(ctx context.Context, db Store, source, nextCursor string, events []Event) error {
	return db.WithTransaction(ctx, func(tx Tx) error {
		for _, event := range events {
			inserted, err := tx.InsertEventOnce(event.Key, event.RecipientKey, event.Kind, event.OccurredAt)
			if err != nil {
				return err
			}
			if !inserted {
				continue
			}
			if event.Kind == "hard_bounce" || event.Kind == "complaint" {
				if err := tx.Suppress(event.RecipientKey, event.Kind, event.OccurredAt); err != nil {
					return err
				}
			}
		}
		return tx.SaveCursor(source, nextCursor)
	})
}

func MaySend(ctx context.Context, db Store, recipientKey string, now time.Time) error {
	allowed, checkedAt, err := db.Eligibility(ctx, recipientKey)
	if err != nil || now.Sub(checkedAt) > 10*time.Minute {
		return ErrSuppressed // Unknown or stale is not permission to send.
	}
	if !allowed {
		return ErrSuppressed
	}
	return nil
}
```

The ten-minute freshness budget is an example service policy, not a universal deliverability rule. Set it from the reset expiry, expected feedback delay, polling interval, and business tolerance for another attempt. For a reset link that expires in 15 minutes, an hourly poller plainly cannot protect the next send inside that window. Measure the age of the last successful reconciliation, not merely whether the cron process is running.

## Three ingestion shapes under failure

Three designs recur. Webhook-only ingestion has the least scheduled work and the shortest normal delay, but recovery needs replay support or another source of truth. Poll-only ingestion is easy to reason about when the provider exposes a cursor or time-bounded event feed, though freshness now depends on schedule health and pagination correctness. A hybrid consumes push events and polls for repair. It costs more integration work, yet it gives the cleanest answer after a missed notification.

| Design | Integration effort | Best fit | Main limitation |
| --- | --- | --- | --- |
| Push only | Lower scheduled-work burden | Feedback sources with documented replay | A missed event can outlive local recovery data |
| Poll only | One worker plus durable cursor state | Low volume and a risk window longer than poll lag | Freshness depends on schedule and pagination health |
| Push plus poll | Two ingestion paths with shared idempotency | Short reset expiry or weak replay guarantees | More failure modes must be tested and observed |

Choose from capabilities, not logos. Amazon SES documents sending authorization and deliverability features within its email service, but an adapter still has to map its feedback and suppression concepts into application-owned states. Other providers may expose webhooks, account-level suppression exports, or message-event searches with different retention and pagination boundaries. Those contracts can change the adapter and recovery window; they should not change the pre-send invariant.

For a small marketplace with low reset volume, poll-only can be adequate if the polling interval fits the risk window and alerts fire before the data becomes stale. A larger system, or one where attackers can trigger repeated reset attempts, usually justifies push plus reconciliation. This advice does not apply unchanged to marketing mail: consent, unsubscribe, and campaign segmentation add policy states that a password-reset pipeline should not invent.

## What should block the rollout?

Alert on outcomes that reveal a broken guardrail: age of the last successful poll, consecutive polling failures, pages processed with an unchanged cursor, feedback deduplication conflicts, queued resets older than their token expiry, and attempted sends blocked by suppression reason. Track bounce and complaint rates as trends, but avoid declaring a universal threshold without the denominator, traffic mix, and provider definitions.

The runbook should start with containment. Pause only the affected delivery queue, preserve incoming feedback, verify credential and cursor health, then replay from the last known-good checkpoint. Do not clear suppressions to make a graph recover. Recovery is complete when the local projection catches up, expired reset jobs are discarded, and a synthetic address exercises request, queue, eligibility, send adapter, and feedback ingestion without using a real customer's mailbox.

Test the ugly ordering. Feed the same complaint twice. Deliver a hard bounce before the send worker records its provider message ID. Crash after the last event is written but before the cursor commit. Return an empty middle page with a continuation cursor. Rotate credentials while a poll is in flight. These cases find more production defects than a happy-path SDK mock.

Deployment needs the same caution. Add the local state and shadow-read it first; ingest feedback next; enable blocking only after lag and mapping are visible. Keep an audited manual override for verified address correction, with expiry and actor identity. A broad "unsuppress all" switch is not a recovery mechanism.

## The steady-state runbook

Use the smallest integration that can keep suppression knowledge fresher than the marketplace's reset-risk window and can prove it after a consumer outage. For many systems, that means local durable state, idempotent feedback application, transactional cursor movement, and a pre-send check. Add push ingestion when polling delay is too large; retain polling when push delivery cannot be replayed reliably.

No delivery API removes the need for this ownership boundary. The application decides whether a password-reset attempt is eligible, the queue carries that decision toward delivery, and reconciliation supplies evidence that the decision remains current. When any link is uncertain, stop the send and page on stale state. Quiet failure is the dangerous mode.

## Sources

- Amazon SES Developer Guide: https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- NIST SP 800-63B, Digital Identity Guidelines: https://pages.nist.gov/800-63-3/sp800-63b.html
