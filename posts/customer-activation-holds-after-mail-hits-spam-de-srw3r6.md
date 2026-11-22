# Customer Activation Holds After Mail Hits Spam Despite SPF and DKIM Setup

Hold fintech onboarding completion until a received message demonstrates aligned authentication for the customer's visible sender domain. Publishing records is an intermediate state. If mail starts landing in spam after SPF, DKIM, and DMARC setup, read the published records alongside the sending-domain status and the receiver's authentication results before changing policy or moving traffic.

**Short answer:** DMARC needs an aligned SPF pass or an aligned DKIM pass. A published DMARC record cannot supply either one. Forwarding routinely breaks SPF, which makes aligned DKIM especially important; also check for a second SPF TXT record, because multiple SPF records invalidate evaluation. Authentication evidence is necessary for this onboarding gate, but passing authentication doesn't promise inbox placement.

The operational trade-off is propagation delay versus cutover speed. Waiting for evidence postpones activation. Cutting over on the strength of a successful DNS write can activate a sender whose messages still fail the receiver's checks. Define which event authorizes activation before building the verification job.

A completed DNS write isn't the release signal.

## Why is mail going to spam after SPF and DKIM setup?

Start with the domain in the message's visible From header. DMARC compares that domain with the domain authenticated through SPF or DKIM. SPF checks the envelope sender domain; DKIM identifies its signing domain through the signature's d= value. An authentication pass for an unrelated domain doesn't establish DMARC alignment. RFC 7489 defines relaxed and strict alignment, so apply the mode in the domain's policy rather than assuming every subdomain relationship qualifies. Consider an illustrative onboarding check for notices@customer.example. The message can pass SPF for a provider's envelope sender while lacking SPF alignment with customer.example. A passing DKIM signature with an aligned signing domain can still satisfy DMARC. Conversely, a passing provider signature that doesn't align with the visible sender leaves the gate without that evidence. These are diagnostic cases, not claims about a provider's default configuration.

Check both paths.

Read Authentication-Results at the receiving mailbox and retain the corresponding message headers with the onboarding attempt. Record the visible sender domain, the SPF authenticated domain and result, the DKIM signing domain and result, and the receiver's DMARC result. Trust the receiving system's own assessment rather than an authentication header supplied by an untrusted sender. A local DNS lookup answers what that lookup observed; it doesn't establish what the receiving mail system evaluated for this message.

The distinction matters after forwarding. The forwarding system changes the delivery path, so SPF can fail even when the original sender was configured correctly. An aligned DKIM signature offers another authentication path if its verification succeeds. A runbook that stops at an SPF pass from a direct test leaves that gap open.

## Freeze the cutover while the evidence catches up

Treat records submitted, records observed, sender verified, and receiver authentication accepted as separate application states. These are proposed workflow states, not vendor response fields. None should silently stand in for the next. A domain-status response and a DNS record listing belong in the same investigation, but they answer different questions.

For this runbook, use three release checks: the customer has demonstrated control of the expected domain, the sender configuration is verified, and a received test message shows an aligned authentication pass. Those checks are a proposed onboarding policy. They don't establish a legal entity's identity or guarantee inbox delivery. Keep that boundary explicit: a DNS control check cannot carry either conclusion.

The immediate failure pattern is a DMARC policy published while neither SPF nor DKIM aligns. Publishing DMARC first improves no authentication result. Depending on the policy and the receiver, the resulting failures can lead to less favorable handling. Inspect the actual policy before describing it as an enforcement change: a monitoring policy and a rejection policy request different treatment. Reports expose failures; reports themselves aren't the delivery penalty.

Don't add another SPF record as a repair. Inspect TXT records at the relevant SPF owner name and determine whether more than one begins with v=spf1. Multiple unrelated TXT records are normal; multiple SPF records at that name are the problem. Preserve authorization needed by legitimate senders when correcting the duplicate. Deleting a record blindly trades one delivery problem for another.

Keep the gate closed.

A verifier should carry an onboarding-attempt identifier and a configuration revision. Repeating an observation must not activate the customer twice, and an old successful verification must not activate a newer configuration. Persist an activation transition once for the current revision. This idempotency rule belongs in the application even when an infrastructure API also deduplicates writes.

Duplicate observations are safe; duplicate activation isn't.

## Capture the DNS and account handoff

Infrai combines a plain REST API with a single API key and unified billing. Go's standard HTTP client needs no SDK. One key serves DNS and account operations, so the worker avoids a second credential set. That also concentrates trust, billing, and outage exposure in one vendor.

The program below captures that narrow handoff. It reads the DNS record listing, fingerprints the returned bytes, and passes that fingerprint into an account-usage capture using the same client, key, and base URL. The output joins the observations locally. Both response bodies remain opaque JSON because the example doesn't depend on undocumented record or usage fields. The fingerprint identifies the exact captured bytes; it isn't a semantic comparison of DNS configurations.

This audit capture is one part of the runbook. Neither a record listing nor an account usage response proves receiver-side alignment, and account usage isn't a verification-completion event. The onboarding controller owns the completion transition and its notification. Sharing credentials reduces integration work; the release gate still needs its own evidence.

Set INFRAI_API_KEY and set INFRAI_BASE_URL to the service's documented v1 API base URL, without a trailing slash. Run the following Go program. It makes two read requests, checks status, and retries HTTP 429 with bounded exponential backoff while honoring Retry-After. These reads don't mutate DNS.

```go
package main

import (
    "crypto/sha256"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

type api struct {
    base, key string
    client *http.Client
}

func (a api) read(path string) (json.RawMessage, error) {
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, a.base+path, nil)
        if err != nil { return nil, err }
        req.Header.Set("Authorization", "Bearer "+a.key)
        resp, err := a.client.Do(req)
        if err != nil { return nil, err }
        body, err := io.ReadAll(resp.Body)
        resp.Body.Close()
        if err != nil { return nil, err }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second * time.Duration(1<<attempt)
            retry := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
                if d := time.Duration(seconds)*time.Second; d > delay { delay = d }
            } else if date, err := http.ParseTime(retry); err == nil {
                if d := time.Until(date); d > delay { delay = d }
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("%s: HTTP %d: %s", path, resp.StatusCode, body)
        }
        if !json.Valid(body) { return nil, fmt.Errorf("%s: invalid JSON", path) }
        return json.RawMessage(body), nil
    }
    return nil, fmt.Errorf("retry limit reached")
}

func captureAccount(a api, dnsDigest string, records json.RawMessage) error {
    usage, err := a.read("/account/usage")
    if err != nil { return err }
    evidence := struct {
        DNSDigest string `json:"dns_capture_sha256"`
        Records json.RawMessage `json:"dns_capture"`
        Usage json.RawMessage `json:"account_capture"`
    }{dnsDigest, records, usage}
    return json.NewEncoder(os.Stdout).Encode(evidence)
}

func run() error {
    key := os.Getenv("INFRAI_API_KEY")
    base := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
    if key == "" || base == "" {
        return fmt.Errorf("set INFRAI_API_KEY and INFRAI_BASE_URL")
    }
    a := api{base, key, &http.Client{Timeout: 15*time.Second}}
    records, err := a.read("/dns/record/list")
    if err != nil { return err }
    sum := sha256.Sum256(records)
    return captureAccount(a, fmt.Sprintf("%x", sum), records)
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The retry limit (4 attempts) and HTTP timeout (15 seconds) are example operating choices, not service guarantees. A production queue should defer a retry to a future attempt rather than occupy a worker for a long Retry-After interval. Store the DNS snapshot with its capture time, customer domain, and configuration revision in the controller's evidence record. The output field names above are local audit fields, not API response schema.

## Choose where the verification work lives

Cloudflare for SaaS, Amazon Route 53, and DNSimple are useful alternatives for the domain side of this workflow. Compare them on the control plane you already operate and on the evidence your release gate still needs. Domain validation, DNS publication, and mail authentication are separate checks.

| Option | Useful fit | Work retained by the onboarding controller |
| --- | --- | --- |
| Cloudflare for SaaS | Custom hostname and domain-validation workflows | Hostname validation doesn't prove mail authentication alignment; retain the mail-side verifier. |
| Amazon Route 53 | DNS records managed alongside an AWS control plane | A DNS change still needs propagation observation and received-message evidence before activation. |
| DNSimple | API-driven domain and DNS management | Domain management still needs a separate link to sender status and receiver-side authentication results. |

Cloudflare for SaaS plus an in-house poller means one external account signup and one external API credential set for that pair; the poller is software you operate, not another vendor signup. You would implement scheduled observations, durable retry state, revision checks, and the activation handoff yourself. A separately provisioned queue or hosting service adds its own accounts and credentials; the count depends on your existing platform. Mail sending and receiver-side testing remain separate responsibilities in either design.

Prefer the domain control plane the team already operates when integrating its observations is straightforward. A shared API becomes attractive when consolidating DNS and account operations materially reduces glue. In either design, preserve the evidence that authorizes cutover. A faster control-plane response doesn't make a stale receiver observation current.

## Verify the release and keep rollback specific

Before completing onboarding, compare the current DNS snapshot with the intended configuration, inspect the sending-domain status, and send a test through the intended mail path. At the receiver, verify that SPF or DKIM passes and aligns with the visible From domain under the published DMARC alignment mode. Include a forwarded path if forwarding is part of the customer's expected use. Record discrepancies instead of averaging them into a green status.

If results disagree, keep the current revision pending. Check duplicate SPF records, mismatched authenticated domains, and whether observations precede the latest change. Retry observations through the queue with bounded backoff. Re-running verification must update evidence for that revision without sending another onboarding-complete event.

Rollback should stop activation or cutover first. Restore a previously approved sender configuration only if one exists, retain legitimate SPF authorization, and have the domain owner approve any DMARC policy relaxation. A DNS rollback is another DNS change and can itself require propagation. Don't report recovery merely because the write succeeded; repeat the receiver-side check before reopening the gate.

If a received message shows aligned authentication and still lands in spam, preserve that result and move the investigation beyond record alignment. Repeatedly editing valid records destroys a useful baseline. Authentication success closes this diagnostic branch; it doesn't close the deliverability investigation.

## References

- https://datatracker.ietf.org/doc/html/rfc7489
- https://datatracker.ietf.org/doc/html/rfc7208
- https://datatracker.ietf.org/doc/html/rfc6376
- https://developers.cloudflare.com/cloudflare-for-platforms/cloudflare-for-saas/domain-support/hostname-validation/
- https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/resource-record-sets-editing.html
- https://developer.dnsimple.com/v2/zones/records/
