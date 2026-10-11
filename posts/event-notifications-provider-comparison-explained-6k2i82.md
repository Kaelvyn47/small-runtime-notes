# Event Notifications Provider Comparison Explained — 4 Webhook Polling Controls in Node.js

For a healthtech signup app, an event notifications provider comparison should favor webhook delivery backed by polling reconciliation, because neither email nor SMS API receipts are a complete compliance record. The page says verification-link evidence has gone quiet. On-call can see accepted API calls, yet cannot tell whether messages were delivered, delayed, suppressed, or merely awaiting a receipt. That is already too late: the first useful alert should have fired when the receipt stream stopped matching the send ledger.

**TL;DR:** Compare notification providers by the evidence they let you retain, reconcile, and explain. Use signed webhooks for prompt state changes, bounded polling to repair gaps, and an internal append-only delivery ledger as operational truth. Track four signals: accepted sends, authenticated receipts, unmatched records, and verification completion. Email versus SMS is then a routing decision, not an excuse to weaken the audit trail.

## The page starts with a missing fact

A useful page names the affected workflow and absent evidence: `signup_verification_receipts_missing`, region, channel, first-seen time, and the number of sends whose state has not advanced. It must not include the verification URL, token, message body, phone number, or email address. Those values increase the cost of a leaked alert without helping triage.

The runbook is short. Check whether accepted-send events still arrive. Check whether receipt authentication failures increased. Compare the oldest unresolved send with normal receipt lag for that route. Then sample internal correlation IDs across the send and receipt ledgers. Do not resend the entire unresolved set from the alert console; retries without an idempotency boundary can create duplicate messages and a second incident.

An API acceptance is evidence that a provider accepted a request, not evidence that a user received or acted on it. SMTP permits successful transfer without proving that a person saw a message. SMS length also changes encoding and segmentation: Twilio documents 160 GSM-7 characters for one segment and 70 UCS-2 characters, with lower limits in concatenated messages. A template edit can therefore alter segmentation without changing application code.

Receipts can lie by omission.

Keep the states separate: `requested`, `accepted`, `delivered`, `failed`, and `verified`. “Verified” belongs to the application because only it observes successful token redemption. Normalize varying provider terms without discarding raw events.

## What evidence would answer an auditor's question?

The smallest defensible record answers who initiated a send, which approved template revision was used, where it was routed, what the provider reported, and when each transition occurred. “Who” can be a service principal; it should not require copying health data into notification logs. Retention and access rules should follow the organization's applicable policy and legal assessment, not a vendor default.

Four signals make that record operational:

1. Accepted sends: requests that crossed the provider boundary, keyed by an application-generated message ID.
2. Authenticated receipts: events that passed signature, timestamp, schema, and deduplication checks.
3. Unmatched records: accepted sends without a terminal receipt, plus receipts that cannot join to a known send.
4. Verification completion: successful, expired, and rejected token redemptions, recorded without the token.

Generate the join key before the first provider call, persist it with the signup transaction or an outbox record, and propagate it through provider metadata where supported. Store the provider message identifier as a secondary key. If failover changes the provider, the application ID remains stable.

Verification links need single-purpose, expiring, high-entropy tokens, with reuse prevented after success. Never log the raw URL or make it a metric label.

## Should a provider comparison favor webhook or polling for event notifications?

Neither transport is the audit record. A webhook is a prompt input to that record; polling is a reconciliation input. Treating a provider response as the database of record makes investigations depend on external retention and mutable presentation.

| Criterion | Signed webhook | Bounded polling | Practical control |
| --- | --- | --- | --- |
| Detection delay | Arrives as events are emitted | Limited by interval and backlog | Page on ledger lag |
| Authenticity | Verify signature over the exact request | Protect query credentials | Retain verification outcome |
| Duplicates | Expect redelivery | Expect overlapping pages | Enforce a unique event key |
| Missing events | Detect reconciliation gaps | May recover exposed states | Join against accepted sends |
| Load | Bursty inbound traffic | Predictable requests | Queue and bound retries |
| Evidence custody | Persist every event | Persist observed transitions | Keep an append-only ledger |

A provider comparison should test these properties with the production account configuration and region. A shortlist might include Twilio, SendGrid, Customer.io, Courier, Knock, or an email API such as Resend, but names do not settle the decision. The relevant boundaries are documented event coverage, webhook authentication, status-query capability, identifier propagation, regional processing terms, retention controls, exportability, and failure semantics. Record citations and test results. Do not infer one product's behavior from another product in the same corporate family.

Push plus pull is usually easier to explain when both exist. Webhooks keep working state fresh. A scheduled reconciler queries only sends older than the expected receipt window and stops after a defined horizon; it does not scan all history forever. If polling is unavailable, reconciliation can identify unresolved entries, but it cannot invent a provider receipt.

There is a real trade-off. Webhook-only delivery is not a fit when the receiver cannot expose a protected, highly available ingress or when the provider does not expose every state needed for reconciliation. Polling-only delivery is a poor fit for time-sensitive verification because detection is bounded by the interval, and aggressive polling adds load while still observing only the states the API retains. Even the combined pattern has a limitation: it costs another ledger, reconciler, signing-key procedure, and on-call runbook. A low-volume workflow with no compliance evidence requirement may rationally accept a simpler design, but a healthtech signup flow should document why any missing control is tolerable.

That maintenance is the price of independent evidence.

## Instrument the receipt before tuning the alert

This focused handler authenticates the exact body before decoding it, rejects stale timestamps, deduplicates on a provider event ID, and appends the transition. Its signature construction is illustrative; production code must implement the selected provider's documented algorithm and key rotation exactly.

```go
package receipts

import (
    "context"
    "crypto/hmac"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "errors"
    "strconv"
    "time"
)

type Event struct {
    EventID string `json:"event_id"`
    MessageID string `json:"message_id"`
    State string `json:"state"`
}

type Ledger interface {
    AppendOnce(context.Context, Event) (bool, error)
}

func Ingest(ctx context.Context, body []byte, stamp, signature string, now time.Time, secret []byte, ledger Ledger) error {
    unix, err := strconv.ParseInt(stamp, 10, 64)
    eventTime := time.Unix(unix, 0)
    if err != nil || now.Sub(eventTime) > 5*time.Minute || eventTime.After(now.Add(time.Minute)) {
        return errors.New("receipt timestamp outside acceptance window")
    }
    mac := hmac.New(sha256.New, secret)
    mac.Write([]byte(stamp + "."))
    mac.Write(body)
    got, err := hex.DecodeString(signature)
    if err != nil || !hmac.Equal(got, mac.Sum(nil)) {
        return errors.New("invalid receipt signature")
    }
    var event Event
    if err := json.Unmarshal(body, &event); err != nil {
        return err
    }
    if event.EventID == "" || event.MessageID == "" {
        return errors.New("receipt missing correlation fields")
    }
    _, err = ledger.AppendOnce(ctx, event)
    return err
}
```

The five-minute replay window is an example policy value, not a universal provider requirement. Set it from the documented scheme, measured clock skew, queue delay, and threat model. Exact choices belong in the runbook.

Deploy the ledger before changing the page. During rollout, shadow the alert and inject safe synthetic events containing no patient data. Test duplicates, reordered states, invalid signatures, delayed receipts, timeouts, and verification arriving before a delayed delivery receipt. Metrics should use bounded labels such as region, channel, and normalized state. Message IDs and recipient identifiers belong in controlled logs, not metric labels.

## Close the loop without paging on normal delay

The earlier signal is a mismatch between accepted sends and authenticated receipts, expressed as count and age. A raw failure-rate alert misses a silent receipt outage because no failures arrive. A raw “no webhooks” alert wakes someone during quiet signup periods. Combine receipt freshness with evidence of recent accepted sends.

Derive thresholds from observed receipt-lag distributions by channel and region, then review them after template, routing, or provider changes. Use a ticket for slow drift and a page for a fast, user-impacting breach. Verification completion can corroborate delivery, but it should not disclose identity and cannot be the sole measure: users may abandon signup after receiving a valid link.

False positives carry compliance cost. Repeated low-value pages train responders to acknowledge without investigating, while broad alert payloads replicate sensitive identifiers into paging systems. A tight threshold can also trigger automated failover or retries that duplicate messages. Page only when evidence shows current signup traffic, sustained inability to account for delivery state, and an action the responder can take. Everything else belongs in reconciliation reports and daytime review.

Require every candidate to pass the same authentication, duplicate, gap, regional, data-minimization, and export tests. Choose the boundary whose evidence can be independently retained and reconciled under the organization's obligations. The logo is not the architecture.

## References

- Resend, “Introduction”: https://resend.com/docs/introduction
- Twilio, “What is the SMS character limit?”: https://www.twilio.com/docs/glossary/what-sms-character-limit
- IETF RFC 5321, “Simple Mail Transfer Protocol”: https://www.rfc-editor.org/rfc/rfc5321
- IETF RFC 9421, “HTTP Message Signatures”: https://www.rfc-editor.org/rfc/rfc9421
- OWASP, “Logging Cheat Sheet”: https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html
- U.S. Department of Health and Human Services, “Security Rule”: https://www.hhs.gov/hipaa/for-professionals/security/laws-regulations/index.html

## Further reading

- OWASP, “Forgot Password Cheat Sheet”: https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- NIST SP 800-63B, “Authentication and Lifecycle Management”: https://pages.nist.gov/800-63-4/sp800-63b.html
