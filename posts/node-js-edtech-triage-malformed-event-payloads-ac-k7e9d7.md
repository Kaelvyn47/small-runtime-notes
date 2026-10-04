# Node.js Edtech Triage: Malformed Event Payloads Across Email and SMS Boundaries

The fastest way to debug a malformed contact-notification payload is to preserve the original event, identify the first boundary that changes it, and replay each stage without sending anything. **Do not begin with the email or SMS provider.** In an edtech support flow, first prove whether intake, queue routing, template binding, or dispatch turned a valid contact submission into an invalid delivery request. This ordering keeps integration effort bounded: one captured fixture can test every adapter.

TL;DR: Record sanitized structural evidence and version identifiers, reproduce the failure against a side-effect-free pipeline, and classify it as decode, route, bind, render, or dispatch. Invalid phone or email values and missing template variables are deterministic defects; retries do not repair them. Preserve the original idempotency key when a corrected event is replayed.

I have been paged by missed jobs and duplicate deliveries. The useful invariant from those incidents is narrow: an uncertain send outcome and an invalid payload are different operational states. Treating both as generic queue failures makes the first unsafe to replay and the second impossible to fix automatically.

## How can Node.js trace a malformed event payload across email and SMS?

Start with one failed request ID and walk backward from the recorded stage, not from a provider error string. A parent may submit a billing question with an email address, a phone number, and a message; the routing rule assigns the school billing queue; the selected template then expects `school_name`, `case_id`, and `reply_channel`. Each transition can be locally valid while the next contract is broken.

The evidence should answer five questions: Could the original JSON be decoded? Which routing-rule version selected the queue? Which template version was bound? What variable names were supplied? Was dispatch attempted, and under which stable idempotency key? Store reason codes and field names, but omit message bodies, addresses, phone numbers, and secrets from logs. My first check is the last recorded successful boundary, because it narrows the replay before anyone touches retry controls. For example, if decode and route both succeeded for `school_id=west-14` but bind rejected `caseID`, the useful fixture contains the route outcome, expected `case_id` key, and template version; it does not need the parent's message or address. That is enough to reproduce contract drift without handling the original contact data again.

Short evidence beats a payload dump.

Use stage-specific outcomes so the runbook points to an owner:

| Stage | Structural evidence | Likely repair | Automatic retry |
|---|---|---|---|
| Decode | Parse failure or unknown field name | Fix the producer or declared contract | No |
| Route | No school-and-topic match | Repair routing data | No |
| Bind | Missing template version or variable name | Align producer and template contract | No |
| Render | Empty required value | Repair or quarantine the event | No |
| Dispatch | Timeout or temporary rejection | Apply bounded backoff with the same key | Policy-dependent |

This sequence also exposes a common false lead. A syntactically plausible email address does not prove that a mailbox exists, and a phone-shaped string does not prove that a handset is reachable. Local validation can establish only the contract the system chose to enforce. Sender authentication and reputation are separate email-delivery concerns; Google's sender guidelines document those controls for delivery to Gmail accounts.

## Turn the failure into a replay fixture

Capture a sanitized envelope before changing code. Keep contract versions, routing inputs, channel presence, and variable names. Replace contact values with structurally equivalent test values. Then execute the same transformations in dry-run mode: no queue publication, no network call, no message send.

Stop there.

The replay should stop at the first violated invariant. If intake accepts `schoolID` but routing reads `school_id`, report that mismatch before checking templates. If routing succeeds and the bound template requires `case_id`, compare the complete variable set before rendering. Scanning rendered text for leftover braces is weak evidence because template syntaxes and escaping rules differ.

For JSON contracts, JSON Schema Draft 2020-12 provides vocabulary for object shape, required properties, conditional validation, and unevaluated properties. A Node.js service can use a conforming validator at intake, but route existence and template-version lookup remain application decisions. Keep those decisions visible in the trace instead of hiding them inside a single “invalid payload” response.

## Make dispatch impossible in the diagnostic path

The safest debugging harness accepts a resolved job and returns findings; it never invokes an adapter. This Go code models the consumer-side contract even when the producer is Node.js. Shared fixtures, rather than shared implementation language, are the integration point.

```go
package notification

import (
	"fmt"
	"net/mail"
	"regexp"
	"sort"
	"strings"
)

var internationalPhone = regexp.MustCompile(`^\+[1-9][0-9]{1,14}$`)

type Job struct {
	RequestID      string
	Queue          string
	Email          string
	Phone          string
	Template       string
	TemplateVars   map[string]string
	RequiredVars   []string
	IdempotencyKey string
}

func Diagnose(job Job) []string {
	var findings []string
	if strings.TrimSpace(job.RequestID) == "" || job.Queue == "" {
		findings = append(findings, "unresolved routing identity")
	}
	if job.Template == "" || job.IdempotencyKey == "" {
		findings = append(findings, "unbound delivery identity")
	}
	if job.Email == "" && job.Phone == "" {
		findings = append(findings, "no reply channel")
	}
	if job.Email != "" {
		address, err := mail.ParseAddress(job.Email)
		if err != nil || address.Address != job.Email {
			findings = append(findings, "email violates canonical job contract")
		}
	}
	if job.Phone != "" && !internationalPhone.MatchString(job.Phone) {
		findings = append(findings, "phone violates international job contract")
	}

	required := append([]string(nil), job.RequiredVars...)
	sort.Strings(required)
	supplied := make([]string, 0, len(job.TemplateVars))
	for key, value := range job.TemplateVars {
		if strings.TrimSpace(value) == "" {
			findings = append(findings, fmt.Sprintf("empty template variable: %s", key))
		}
		supplied = append(supplied, key)
	}
	sort.Strings(supplied)
	if strings.Join(required, "\x00") != strings.Join(supplied, "\x00") {
		findings = append(findings, "template variable set differs")
	}
	return findings
}
```

The regular expression represents the E.164 numbering shape used by this job contract: a plus sign and at most 15 digits, with the first digit nonzero. The ITU specification is the authority for the numbering plan. Parsing and normalization must happen before this diagnostic function. Likewise, `mail.ParseAddress` is useful for a local syntax policy; it cannot verify mailbox ownership or delivery.

Exact variable comparison is intentionally strict. It catches a misspelling such as `schoolName` when the versioned contract requires `school_name`. If a template supports optional variables, compare supplied keys against separate required and permitted sets. Do not weaken the check to accept arbitrary extras.

## Compare fixes by integration effort

There are three reasonable repair scopes. Patching only the failing adapter is the smallest change, but it duplicates normalization and template assumptions across email and SMS. Adding checks only to the Node.js producer catches defects earlier, yet other producers can still publish incompatible jobs. A channel-neutral diagnostic contract plus shared fixtures takes more initial coordination, but it gives intake, routing, and consumers the same failure vocabulary without tying them to one delivery SDK.

This approach has limits.

Choose the narrow patch when there is one producer, one consumer, and deliberately best-effort delivery. Choose shared fixtures when multiple teams change routing rules or templates independently. The limitation is maintenance: versioned fixtures and a dry-run path add code that every contract change must update. They are not appropriate for a low-consequence feedback form whose partial submissions already go directly to manual review. The extra integration work pays for itself operationally only if a failed learner, parent, or billing contact must be traceable and replayable.

Keep routing separate from transport. A missing mobile number may change the available reply channel; it must not move a billing question into the academic-support queue. Queue ownership comes from school and topic. Channel selection happens after that decision.

## Verify the repair without creating another incident

Add the captured fixture to contract tests at every transformation boundary. Test malformed JSON, unknown fields, absent route mappings, invalid canonical contact values, missing variables, extra variables, and empty required values. Then test the uncomfortable case: dispatch times out after the remote system may have accepted the message. That result is uncertain, not malformed, so any replay must retain the original idempotency identity and follow the adapter's documented retry semantics.

Deploy the producer and consumer changes in a compatible order. A practical transition is to make consumers understand both contract versions, move producers to the new version, observe old-version traffic drain, and only then remove the old path. The trace must record the actual schema, routing-rule, and template versions or the next investigation will guess.

Watch counts by stage and low-cardinality reason code. A sharp change after a deployment suggests contract drift; failures isolated to one school-topic pair suggest routing data; repeated dispatch attempts with no increase in accepted intents suggest acknowledgment or retry behavior. Those are diagnostic hypotheses, not proof, so confirm them with one sanitized fixture before replaying a batch.

Authentication events require a different risk review. NIST SP 800-63B discusses out-of-band authenticators and their security properties. A support notification is not an authenticator, and its email or SMS assumptions should not be copied into password recovery without that analysis.

The operating rule is simple: reproduce before retrying, name the first broken boundary, and keep diagnosis side-effect-free. **A corrected payload may be replayable; an uncertain delivery needs idempotency-aware handling.** That distinction prevents a malformed contact event from becoming either a missed support request or a duplicate message.

## Sources

- https://json-schema.org/draft/2020-12/json-schema-core
- https://www.rfc-editor.org/rfc/rfc5322
- https://www.itu.int/rec/T-REC-E.164
- https://support.google.com/a/answer/81126
- https://pages.nist.gov/800-63-3/sp800-63b.html
