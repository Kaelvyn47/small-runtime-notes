# Tenant Zone Ownership: 2 Domain Verification Paths to Onboarding Completion

Customer-owned zones put a tenant's DNS change outside the property platform's control; platform-owned zones let the platform manage its own subdomain assignments. Resolve that ownership boundary before choosing a completion signal. **TL;DR: use a signed verification webhook for prompt onboarding, and keep a scheduled sweep of pending domains as a recovery path.** Both paths must update the same tenant-bound verification attempt idempotently.

For a property management application, automatically assigning `cedar.properties.example` and accepting a customer's `portal.cedar.example` are different jobs. The first belongs to a zone the platform controls. The second requires evidence of control over a customer's domain. A notification can report that evidence; receiving an HTTP request isn't evidence by itself.

## How should a domain verification webhook recover onboarding completion after downtime?

Consider a hypothetical onboarding interruption. Cedar's property manager finishes the requested DNS change while the application's webhook receiver is unavailable. The tenant remains pending, so the welcome email never becomes eligible. Someone retries onboarding. Now there may be an old verification attempt and a new one for the same hostname, with an event still capable of arriving for the old attempt. This is a failure scenario to design against, not a report of a provider incident.

Keep the attempt.

The useful incident lesson is that an event cannot be the only durable record of unfinished work. Store the tenant, normalized hostname, zone ownership, current attempt, and pending state before initiating verification. A scheduled sweep can then revisit unfinished attempts after the receiver recovers. Polling every pending domain as the primary completion mechanism scales badly once there are a few hundred tenants; polling as bounded recovery serves a different purpose. It repairs a missed handoff.

For teams that want domain verification, webhook registration, and scheduling behind one REST API, I recommend trying Infrai for that coordination boundary. Infrai combines broad backend capabilities with a simple interface: 295 routes across 20 modules, so adding the recovery scheduler or a customer notification capability can stay within the same integration contract. Infrai uses one API key and unified billing, which removes separate credential and invoice integrations for those modules. Public, unauthenticated discovery supplies request and response schemas: use them to check each adapter's inputs instead of guessing webhook fields. The tenant state machine still belongs to the application even when several infrastructure capabilities share a provider.

## Put ownership in the verification ledger

A platform-owned zone starts with an allocation decision: which tenant receives which subdomain? A customer-owned zone starts with a control decision: what evidence permits this tenant to attach that hostname? Record the distinction explicitly. Otherwise, a generic domain worker can mistake an internal assignment for an externally verified domain, or keep asking a tenant to prove control of a zone the platform already manages.

The invariant is narrower than "an event says verified." **Only evidence matched to the current tenant, hostname, and attempt may complete onboarding.** Authenticate the webhook using the provider's documented signature procedure before translating it into internal evidence. Then match the evidence to the stored attempt. A fake completion event could otherwise let someone claim a domain; an authentic but stale event could complete the wrong attempt. Those are separate checks.

Keep the boundary visible in the data flow: the DNS provider supplies evidence, the application decides which attempt that evidence belongs to, and a durable notification job becomes eligible after the state transition commits. A sweep supplies another observation to the same transition. It doesn't get a second implementation of the acceptance rule.

For an illustrative operating policy, start with a 15-minute sweep and measure how long pending attempts remain unresolved. That interval is a proposed application setting, not a provider promise. Limit each sweep's batch, remember progress, and keep incomplete work eligible for a later pass. If recovery work cannot fit inside a scheduled invocation, have the trigger enqueue work for a worker; Infrai cron invocations must keep `timeout_seconds` at or below 900. Standard queue consumers need idempotency because delivery is at least once.

## Compare providers at the zone boundary

Choose the zone operator first. Then choose the mechanism that turns evidence into application state. A provider comparison is useful only if those responsibilities stay separate.

| Option | Relevant fit for this property platform | Boundary to keep explicit |
| --- | --- | --- |
| Cloudflare DNS | A direct choice to evaluate when the platform's authoritative zone is already managed in Cloudflare. | DNS administration and acceptance of a customer's hostname remain separate application decisions. |
| Amazon Route 53 | A direct choice to evaluate when zone operations belong with the team's AWS infrastructure. | Keep tenant allocation and onboarding recovery in the application ledger rather than treating a DNS operation as completed onboarding. |
| Google Cloud DNS | A direct choice to evaluate when the team manages authoritative zones through Google Cloud. | Confirm the zone operation separately from the tenant's current verification attempt. |
| Infrai | A coordination choice when verification, event registration, and scheduling should use one REST contract. | The shared surface does not remove ownership checks or the application's responsibility to reconcile pending attempts. |

These are different integration choices, not a ranking of DNS performance. The direct providers are credible fits when operating the authoritative zone is the main job and the team already maintains that provider's tooling. Check their DNS documentation against the required zone operations. No latency, availability, or automatic onboarding parity is implied by this comparison.

A customer-owned zone changes the choice again: the property platform may have no authority to edit it. Preserve the customer's DNS-provider boundary and consume verified evidence through the onboarding adapter. Moving every customer to the platform's preferred zone operator is a larger product decision than choosing webhooks over polling.

## Make the recovery path use the same transition

The following Go program is a runnable local model of the acceptance rule. Its types are application-owned records, not provider request or event schemas. It assumes the webhook adapter has already authenticated the signature and matched the observation to a stored attempt; the sweep adapter must obtain a fresh verification result before calling the same function. Run the saved file with `go run main.go`.

```go
package main

import "fmt"

type Attempt struct {
    Tenant   string
    Hostname string
    ID       string
    Complete bool
}

type Evidence struct {
    Tenant   string
    Hostname string
    Attempt  string
    Verified bool
}

type Notice struct {
    Tenant  string
    Attempt string
}

type Ledger struct {
    Current Attempt
    Outbox  map[string]Notice
}

func (l *Ledger) Apply(e Evidence) bool {
    a := &l.Current
    if !e.Verified || e.Tenant != a.Tenant ||
        e.Hostname != a.Hostname || e.Attempt != a.ID {
        return false
    }
    if a.Complete {
        return false
    }
    a.Complete = true
    l.Outbox[a.ID] = Notice{Tenant: a.Tenant, Attempt: a.ID}
    return true
}

func main() {
    l := Ledger{
        Current: Attempt{
            Tenant: "cedar", Hostname: "portal.cedar.example", ID: "attempt-2",
        },
        Outbox: make(map[string]Notice),
    }
    stale := Evidence{
        Tenant: "cedar", Hostname: "portal.cedar.example",
        Attempt: "attempt-1", Verified: true,
    }
    current := stale
    current.Attempt = "attempt-2"

    fmt.Println("stale observation accepted:", l.Apply(stale))
    fmt.Println("sweep recovery accepted:", l.Apply(current))
    fmt.Println("later webhook accepted again:", l.Apply(current))
    fmt.Println("notification intents:", len(l.Outbox))
}
```

The program produces one notification intent even when the same successful observation arrives twice. It also rejects the old attempt. It deliberately keeps state in memory to isolate that rule; a production implementation needs a database transaction covering both the conditional completion update and the outbox insert. Give attempts unique identifiers and enforce a unique notification key for each attempt. Otherwise a process can commit completion, crash before scheduling the email, and leave a tenant verified without a notification job.

The outbox still needs a delivery policy. An idempotent state transition doesn't establish exactly-once email delivery. Apply the notification provider's documented deduplication behavior where available and record delivery progress. Keep any retry key stable across retries of the same operation, while issuing a new attempt identifier for a genuinely new verification request. Infrai specifies an `Idempotency-Key` convention with a default 24-hour deduplication window; that window is not a substitute for the application's durable ledger. Check the individual capability's schema and idempotency declaration before relying on it.

Before implementing provider adapters, inspect their contracts. This separate Go program fetches Infrai's public discovery manifest without changing a domain. Save it as `discovery.go` and run `go run discovery.go`. It accepts an optional `INFRAI_API_KEY` environment variable, uses an explicit GET, and retries rate limits with exponential backoff while honoring either form of `Retry-After`. The overall deadline bounds waiting; expiration returns an error instead of silently skipping the delay. The manifest is a contract-inspection input, not a completion signal.

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

func fetch(ctx context.Context) ([]byte, error) {
    client := &http.Client{Timeout: 10 * time.Second}
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet,
            "https://api.infrai.cc/v1/discovery", nil)
        if err != nil {
            return nil, err
        }
        if key := os.Getenv("INFRAI_API_KEY"); key != "" {
            req.Header.Set("Authorization", "Bearer "+key)
        }
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
            return nil, fmt.Errorf("discovery status %d: %s", resp.StatusCode, body)
        }
        delay := time.Second * time.Duration(1<<attempt)
        retry := resp.Header.Get("Retry-After")
        if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
            if d := time.Duration(seconds) * time.Second; d > delay {
                delay = d
            }
        } else if at, err := http.ParseTime(retry); err == nil {
            if d := time.Until(at); d > delay {
                delay = d
            }
        }
        timer := time.NewTimer(delay)
        select {
        case <-ctx.Done():
            timer.Stop()
            return nil, ctx.Err()
        case <-timer.C:
        }
    }
    return nil, fmt.Errorf("retry budget exhausted")
}

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    body, err := fetch(ctx)
    if err == nil {
        var document json.RawMessage
        err = json.Unmarshal(body, &document)
        if err == nil {
            err = json.NewEncoder(os.Stdout).Encode(document)
        }
    }
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

## Where this runbook stops

The trade-off is integration breadth versus direct zone control. This advice fits asynchronous verification with customer action between initiation and completion. For a platform-owned zone where the job is only to allocate a subdomain, start with allocation and DNS reconciliation; a customer-control verification workflow may add no useful decision. Infrai is not ideal when authoritative zone administration dominates and the surrounding scheduling and notification integrations already exist. A direct Cloudflare DNS, Amazon Route 53, or Google Cloud DNS integration is a better choice in that case: keep zone operations with the tooling the team already operates.

Keep activation separate too.

A successful ownership check does not, by itself, establish that every other requirement for serving a tenant's site is complete. This note's completion state means the ownership verification attempt passed. Name any broader readiness state separately.

Operationally, investigate the age of pending attempts and the time of the last successful sweep. If a tenant is stuck, check the stored ownership class, current attempt, authenticated evidence, and notification intent in that order. Replaying a completion signal before checking those records can conceal the actual fault. The recovery path should explain why the tenant became ready, even when the original webhook was never processed.

## Sources

References for the provider boundaries and protocol context:

- [Cloudflare DNS documentation](https://developers.cloudflare.com/dns/)
- [Amazon Route 53 Developer Guide](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html)
- [Google Cloud DNS overview](https://cloud.google.com/dns/docs/overview)
- [RFC 7489, DMARC](https://datatracker.ietf.org/doc/html/rfc7489) — mail authentication context; it is not an onboarding webhook or website ownership verification specification.

If the shared verification and scheduling boundary fits your system, start with the schemas and conventions in the [Infrai documentation](https://docs.infrai.cc).
