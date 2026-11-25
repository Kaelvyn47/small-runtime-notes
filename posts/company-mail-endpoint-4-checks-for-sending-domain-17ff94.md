# Company Mail Endpoint: 4 Checks for Sending Domain DNS Drift

A successful DNS write is too early to declare a school company's mail domain ready. TL;DR: expose one internal endpoint that records the intended domain configuration, then reconcile mail provisioning, DNS publication, observation, and verification as separate stages. Compare the published records with that saved intent before activating the domain. MX establishes incoming mail routing; it does not, by itself, authenticate outgoing mail.

For an edtech company moving its company mail, the immediate job is concrete: publish the receiving provider's required MX records without mistaking that change for completed sending-domain setup. The endpoint can own the whole workflow. Its response needs to distinguish acceptance from completion.

## How should one internal endpoint set up a sending domain?

Consider a hypothetical production change, bounded to one company domain and one interrupted worker. For this runbook, I would submit a request for `school.example`, let the worker save the provider's required records, and interrupt it after publication but before it saves verification. That is a test scenario, not an account of a past outage. The recovery question is whether the next execution can recognize existing work and continue.

Check the records.

A second execution that blindly creates the mail configuration again risks turning a recoverable interruption into conflicting intent. A worker that treats its last local status as proof of DNS state has the opposite problem: it can report completion while the observed records disagree with the approved configuration. The useful incident lesson is an invariant: activation belongs to a particular intent revision and its verified observations, never merely to a completed queue delivery.

The company-mail change also exposes a scope trap. Receiving and sending are different capabilities. MX records direct incoming mail toward receiving hosts. SPF, DKIM, and DMARC concern authentication and policy for messages using a domain. DMARC evaluates alignment with the visible From domain through SPF or DKIM; an MX comparison cannot establish that alignment. RFC 7489 describes those authentication relationships and the policy discovery process. Keep the routing check and the sending check distinct, even when one request starts both.

Use four explicit checks: approved intent, published routing, published authentication records, and mail verification for the same revision. This is a runbook design, not a DNS protocol requirement. It gives the responder four things to inspect when a job stops halfway through.

## Save intent before scheduling external work

Give the internal endpoint one responsibility at acceptance: durably record what should become true. An illustrative request carries the company domain, a receiving configuration identifier, a sending configuration identifier, and an idempotency key. The configuration identifiers refer to approved server-side settings; the caller should not be able to attach arbitrary record changes to a company-mail operation. Authenticate and authorize the caller against the domain before accepting the change. Persist the intent and a durable work marker together. A scheduler can then discover pending revisions even if the process exits before delivering a queue message. Return an operation identifier with a pending state. Reserve an active state for the later checks. This adds state management, but it makes an interrupted request inspectable instead of leaving an operator to infer what happened from a timeout. A repeated key with the same request should return the same operation. A repeated key with different intent should fail explicitly. Worker redelivery needs its own protection: use a stable provisioning identifier where the external interface supports one, or discover an existing object before attempting creation.

An HTTP idempotency key at your boundary cannot confer idempotency on an unrelated mail interface. That boundary needs an explicit recovery rule.

Keep the desired record set attached to the revision that produced it. Store record owner, type, value, and any required preference or selector information. Compare MX records as structured data: preference and exchange both matter. Compare an RRset without depending on answer order, and normalize DNS names consistently. Don't apply that name normalization indiscriminately to TXT content. Preserve the distinction between DNS presentation syntax and the logical record value.

For `school.example`, an example approved MX entry might have preference 10 and exchange `mx.school.example`. These are illustrative names and values, not a receiving service or deployable mail configuration. Obtain the actual targets and authentication material from the approved mail configuration. Guessing a DKIM selector or copying an SPF value from another domain defeats the intent check before the worker starts.

## Reconcile the difference before declaring success

A synchronous endpoint is attractive because the caller sees one result. The trade-off is the time between writing DNS and observing the intended state: caching can keep earlier answers visible, and external provisioning can require more than one attempt.

Keeping the request open doesn't remove either condition. A queued reconciler gives the operation a durable place to wait, at the cost of a status model and recovery logic.

A periodic audit takes that model further. It can detect a later manual edit after initial activation, but it creates continuing query and review work. For company mail, that can be justified because the decision axis is ongoing drift, not just whether the original write returned successfully. Set the audit cadence from the team's response needs and query budget; no universal interval follows from DNS.

The preventative code path below is deliberately narrow. It gates activation against one immutable revision using already normalized, complete record sets. It doesn't implement a DNS client, a mail service, or the write plan. The caller must supply observations from the intended zone through a trusted lookup path and distinguish lookup failure from an observed empty RRset. A nil lookup result must never stand in for successful absence.

```go
package onboarding

import (
    "errors"
    "slices"
)

type Record struct {
    Owner string
    Type  string
    Value string
}

type Intent struct {
    Revision uint64
    Records  []Record
}

type Observation struct {
    Revision uint64
    Records  []Record
    Complete bool
}

func sameRecords(want, got []Record) bool {
    less := func(a, b Record) int {
        if a.Owner != b.Owner {
            if a.Owner < b.Owner {
                return -1
            }
            return 1
        }
        if a.Type != b.Type {
            if a.Type < b.Type {
                return -1
            }
            return 1
        }
        if a.Value < b.Value {
            return -1
        }
        if a.Value > b.Value {
            return 1
        }
        return 0
    }
    a, b := slices.Clone(want), slices.Clone(got)
    slices.SortFunc(a, less)
    slices.SortFunc(b, less)
    return slices.Equal(a, b)
}

func Ready(i Intent, o Observation, verifiedRevision uint64) error {
    if i.Revision == 0 || len(i.Records) == 0 {
        return errors.New("missing approved intent")
    }
    if !o.Complete || o.Revision != i.Revision {
        return errors.New("observation pending for current revision")
    }
    if !sameRecords(i.Records, o.Records) {
        return errors.New("published records differ from intent")
    }
    if verifiedRevision != i.Revision {
        return errors.New("mail verification pending for current revision")
    }
    return nil
}
```

Here, `Value` contains a canonical logical value, including MX preference and exchange for an MX entry. The inputs cover only the record sets managed by this operation, with duplicates removed during normalization. Passing the whole zone would make unrelated records look like drift. Passing only the expected members would hide extra records in a managed RRset. Neither is a useful observation.

The activation write still needs a concurrency guard. Commit active status only if the saved intent revision equals the revision just checked. Otherwise a newer request could arrive after `Ready` returns and inherit an older verification result. A transaction or conditional update can enforce that guard. A lease can reduce simultaneous workers; it doesn't replace the revision comparison.

## Make the runbook explain the wait

Record the operation identifier, intent revision, current stage, latest observation time, and next retry time. Preserve the reason for a mismatch without placing credentials or private signing material in logs. An operator needs to distinguish a missing MX entry, an unexpected MX exchange, authentication material that differs from intent, and a mail verification step that has not completed. One generic pending label loses that distinction.

Observe authoritative publication separately from recursive lookup results. DNS caching means those observations can differ temporarily. An authoritative match is evidence of publication; it doesn't prove that every resolver has stopped serving an earlier answer. Retain the prior TTL and observation history when available, and avoid promising a universal propagation deadline.

Retry transient lookup or transport failures with bounded backoff. Stop automated retries on an authorization failure or an invalid approved configuration until someone corrects the cause. After a configured observation deadline, move the operation into a review state with its evidence intact. Don't keep writing identical records just because a resolver still serves a cached answer.

Test the interruption points before a company-mail change: after mail provisioning, after DNS publication, and before the final conditional activation write. Also test a duplicate queue delivery and a new intent revision arriving during verification. The expected result is the same approved external configuration and an accurate operation state. These tests exercise recovery behavior; counting successful HTTP responses would miss the central failure.

Keep routing changes scoped. A record replacement should preserve unrelated RRsets and require review for unexpected existing company MX targets. DNS publication is not a mailbox migration plan: receiving readiness, mailbox data, and any transition arrangement need their own acceptance criteria. A saved prior record set helps prepare a reversal, but cached answers mean reversal isn't an instantaneous undo.

## Where should this workflow stop?

If the company only wants incoming mail routing, don't make sending verification a prerequisite for that narrower operation. Model receiving and sending readiness separately. If administrators control DNS outside this system, the endpoint can record intent and verify their publication, but it cannot honestly claim to have performed the writes.

A verified sending domain also doesn't guarantee inbox placement. DMARC addresses domain authentication and policy; it doesn't certify message content or every receiver's delivery decision. Likewise, introducing a stricter DMARC policy should be a reviewed change informed by authentication coverage and reporting, not a side effect of finishing an MX task.

The stopping rule is operationally useful: activate the requested capability only when the approved revision matches the observed records and the relevant mail checks have completed. Keep auditing if continued agreement matters. For a school company's mail, the durable artifact is that evidence of agreement, attached to an operation the next responder can understand.

## References

The protocol references below support the routing, authentication, and caching distinctions; the revision gates and retry policy are proposed application design.

## Sources

- https://datatracker.ietf.org/doc/html/rfc7489 — DMARC authentication alignment, policy, and reporting.
- https://datatracker.ietf.org/doc/html/rfc5321 — SMTP routing and MX processing.
- https://datatracker.ietf.org/doc/html/rfc1034 — DNS concepts, authoritative data, and caching.
- https://datatracker.ietf.org/doc/html/rfc1035 — DNS record representation and MX record fields.
