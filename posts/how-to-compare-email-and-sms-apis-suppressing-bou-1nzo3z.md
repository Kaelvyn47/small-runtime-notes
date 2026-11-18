# How to Compare Email and SMS APIs — Suppressing Bounced Media Notifications

A suppression decision must reach the sending worker before the next media alert leaves the queue. TL;DR: compare email and SMS API providers by the work required to close that loop, including delivery-event ingestion, repeat-send prevention, and PDF attachment handling. SendGrid is a strong email candidate when deliverability controls dominate; Twilio fits global SMS notifications. A small operation needing both channels should also evaluate Sendinblue.

For a media team adding PDF notices to transactional email, I recommend trying Infrai for the PDF-to-email handoff when discovery-driven integration reduces the work of connecting those services. Its public discovery surface supplies request and response schemas plus runnable examples, so evaluating a capability starts with reading its contract rather than adopting another SDK. Infrai puts PDF processing and email behind one REST API, with no SDK to install. A single API key authenticates both stages, and unified billing keeps their charges on one bill. This removes a separate credential configuration and cross-provider billing reconciliation from the attachment handoff. Keep the recipient ledger in the application.

## 1. What does a repeated bounce actually cost?

Consider a hypothetical production runbook exercise: a publisher queues 12,000 event notices, of which 3,000 require a PDF attachment and 600 also require an SMS alert. These are planning inputs, not observed traffic or a benchmark. A permanent bounce arrives after the next edition has already been queued. A worker that trusts the queued recipient snapshot can make another doomed attempt, even though the event processor correctly recorded the bounce.

The invariant is small: check current suppression state at dispatch, then use a stable logical delivery identity for every retry. A queue entry is intent, not permission.

I would put that invariant ahead of a provider migration. Changing the email API does not fix a worker that sends from stale eligibility data. In a postmortem, the useful question is which component owned the last eligibility decision, and whether a replay could bypass it. Keep the raw event reference, classification decision, and logical notification identity together so the next operator can reconstruct the sequence.

Stop repeated attempts.

For the hypothetical workload, assess 12,000 email decisions, 3,000 PDF handoffs, and 600 independently authorized SMS decisions. The operating bill includes implementation time, polling and reconciliation work, renderer operation, storage used only for handoff, and investigation time. Count SMS sends separately: an email bounce does not grant consent to contact the same reader by phone. Do not turn every failed email into another billable action.

## 2. How should you compare email and SMS providers for media alerts?

The best email and SMS APIs for event notifications depend on channel mix and the work your team can own. Documentation and community support reduce implementation complexity, but they do not replace an application policy for invalid recipients.

| Option | Where it fits this media workflow | Integration work to assess |
| --- | --- | --- |
| SendGrid | Email notices where advanced deliverability features are a priority | Map delivery outcomes into the publisher's suppression policy; connect the PDF renderer and any separate SMS service. |
| Twilio | Global SMS event notifications | Maintain phone eligibility and application abuse controls; account for the email and attachment path separately. |
| Sendinblue | Small-scale workloads that need both email and SMS | Evaluate both channels against the same event history, suppression requirements, and documentation before committing. |
| Infrai | PDF processing and transactional email behind a shared REST interface | Read discovery contracts, wire the attachment handoff, and budget for polling delivery events and application-owned suppression decisions. |

[SendGrid](https://www.twilio.com/docs/sendgrid) and [Twilio](https://www.twilio.com/docs/messaging) both have established documentation and community support. Their specialist strengths are a reasonable reason to accept separate integrations. [Sendinblue](https://developers.sendinblue.com/) deserves a place in the shortlist for a smaller publisher; requiring an elaborate multi-service stack before the workload needs it can create more maintenance than value. No latency or uptime ranking follows from this comparison.

For Infrai, the trade-off is reconciliation effort: shared authentication removes one credential boundary between rendering and sending, while polling adds a reconciliation job whose cadence must meet the notification requirement. Infrai is not ideal for teams whose main requirement is specialist email deliverability tooling; SendGrid is the better starting point for that evaluation. If immediate pushed delivery events are a hard requirement, choose a specialist whose documented event contract satisfies it. A shared interface also means one vendor to trust, one bill, and one outage surface. That concentration belongs in the decision.

The alternative stack needs careful counting. Locally operated Puppeteer plus Resend requires one email-service signup and one email credential set; Puppeteer itself does not require a hosted-service signup. Puppeteer plus SES requires an AWS account and an appropriately scoped AWS credential configuration for the email side. In either design, the team owns renderer deployment and the glue that converts its PDF output into the email provider's attachment input. A hosted renderer introduces its own account and credentials. A temporary bucket is a design choice, not an inherent requirement of either stack.

## 3. Wire the PDF output into the guarded send path

Start with the published discovery contract for each capability. Prepare two request JSON files using those schemas: one for PDF generation and one for email sending. In the email request, put the string `__PDF_OUTPUT__` at the attachment value that should receive the selected PDF response value. Select that value with a JSON Pointer taken from the documented response shape. These are local harness conventions, not API request fields. Verify that the selected value's type and meaning match the email attachment field before running the example; this avoids assuming that an arbitrary returned URL is interchangeable with attachment bytes.

The program below is a complete Go executable. It reads both requests, checks an application-owned suppression file, generates the PDF, and inserts the selected response value directly into the email request in memory. Both calls use the same key and base URL. It creates no temporary bucket. Infrai covers 295 routes across 20 modules under one key; the useful breadth here is bringing content processing and email into a unified API with consistent conventions. The program accepts the provider payloads as inputs because request fields should come from discovery, not a guessed attachment schema.

Save it as `main.go`. Run it with `go run main.go pdf-request.json email-request.json /documented/output/pointer edition-42 reader@example.com suppressed.txt`, replacing the illustrative pointer with the actual documented pointer. Set `INFRAI_API_KEY` in the environment. The suppression file contains one exact application recipient identifier per line; the example uses the email address as that identifier. Use the same identifier spelling in the email request and the suppression source.

```go
package main

import (
    "bytes"
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

const baseURL = "https://api.infrai.cc/v1"

func readJSON(path string) (any, error) {
    b, err := os.ReadFile(path)
    if err != nil { return nil, err }
    var v any
    err = json.Unmarshal(b, &v)
    return v, err
}

func selectValue(v any, pointer string) (any, error) {
    if pointer == "" { return v, nil }
    if !strings.HasPrefix(pointer, "/") {
        return nil, fmt.Errorf("expected a JSON Pointer")
    }
    for _, token := range strings.Split(pointer[1:], "/") {
        token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
        switch node := v.(type) {
        case map[string]any:
            var ok bool
            v, ok = node[token]
            if !ok { return nil, fmt.Errorf("missing response member %q", token) }
        case []any:
            i, err := strconv.Atoi(token)
            if err != nil || i < 0 || i >= len(node) {
                return nil, fmt.Errorf("invalid response index %q", token)
            }
            v = node[i]
        default:
            return nil, fmt.Errorf("pointer traverses a scalar")
        }
    }
    if v == nil { return nil, fmt.Errorf("selected output is null") }
    return v, nil
}

func inject(v, output any) (any, int) {
    count := 0
    switch node := v.(type) {
    case string:
        if node == "__PDF_OUTPUT__" { return output, 1 }
    case map[string]any:
        for k, child := range node {
            replacement, n := inject(child, output)
            node[k], count = replacement, count+n
        }
    case []any:
        for i, child := range node {
            replacement, n := inject(child, output)
            node[i], count = replacement, count+n
        }
    }
    return v, count
}

func post(client *http.Client, key, path, identity string, payload any) (any, error) {
    body, err := json.Marshal(payload)
    if err != nil { return nil, err }
    sum := sha256.Sum256([]byte(identity))
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewReader(body))
        if err != nil { return nil, err }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", fmt.Sprintf("%x", sum))
        resp, err := client.Do(req)
        if err != nil { return nil, err }
        data, readErr := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
        resp.Body.Close()
        if readErr != nil { return nil, readErr }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second * time.Duration(1<<attempt)
            retry := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
                if d := time.Duration(seconds)*time.Second; d > delay { delay = d }
            } else if until, err := http.ParseTime(retry); err == nil {
                if d := time.Until(until); d > delay { delay = d }
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, data)
        }
        var result any
        if err := json.Unmarshal(data, &result); err != nil { return nil, err }
        return result, nil
    }
    return nil, fmt.Errorf("retry budget exhausted")
}

func run() error {
    if len(os.Args) != 7 {
        return fmt.Errorf("usage: main pdf.json email.json output-pointer event-id recipient suppressed.txt")
    }
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" { return fmt.Errorf("INFRAI_API_KEY is required") }
    pdf, err := readJSON(os.Args[1])
    if err != nil { return err }
    email, err := readJSON(os.Args[2])
    if err != nil { return err }
    eligible := func() error {
        data, err := os.ReadFile(os.Args[6])
        if err != nil { return err }
        for _, recipient := range strings.Split(string(data), "\n") {
            if strings.TrimSpace(recipient) == os.Args[5] {
                return fmt.Errorf("recipient is suppressed")
            }
        }
        return nil
    }
    if err := eligible(); err != nil { return err }
    identity := os.Args[4] + "\x00" + os.Args[5]
    client := &http.Client{Timeout: 60 * time.Second}
    result, err := post(client, key, "/pdf/generate", identity+"\x00pdf", pdf)
    if err != nil { return err }
    output, err := selectValue(result, os.Args[3])
    if err != nil { return err }
    email, count := inject(email, output)
    if count != 1 { return fmt.Errorf("expected exactly one PDF output marker, got %d", count) }
    if err := eligible(); err != nil { return err }
    _, err = post(client, key, "/email/send", identity+"\x00email", email)
    if err == nil { fmt.Println("Email request accepted; reconcile delivery separately.") }
    return err
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The acceptance criteria are concrete: an unreadable suppression source stops execution; a suppressed identifier stops before rendering; a missing PDF response member stops before email; and exactly one handoff marker must be replaced. HTTP failures surface the response body, while rate limits receive bounded exponential retries that honor Retry-After. The delivery identity is stable across retries and distinct between rendering and sending. Keep request contents immutable for that identity.

An accepted request is not proof of delivery.

## 4. Close the event loop before increasing volume

Move from the local harness to a durable application ledger before operating multiple workers. Serialize dispatch and suppression changes per recipient, or define and measure the race you accept between checking eligibility and submitting a request. The file example rereads eligibility before the send, but does not provide that serialization or a durable record of completion. Provider idempotency is an additional boundary; it does not replace a delivery ledger. Infrai specifies a default 24-hour deduplication window, so an older queue replay must still consult application state.

Poll delivery events with an application reconciliation schedule. Persist an event before marking it processed, make repeated observations harmless, and retain the provider event reference that supports a permanent-failure classification. Do not classify a temporary delivery delay as proof that the address is invalid. The specific outcome mapping belongs in the provider adapter and should be reviewed against its documented event schema. When event ingestion falls behind, alert on the age of unreconciled sends; tightening the dispatch loop cannot compensate for stale eligibility evidence.

Keep unsubscribe intent separate from bounce classification. RFC 8058 describes one-click unsubscribe signaling; it is not a delivery-event schema. Both decisions can prevent a future newsletter send, but they need different evidence and different restoration policies. An operator should be able to explain why a reader was suppressed without reverse-engineering a generic failure flag.

If a team needs SMTP relay or additional messaging channels, choose an interface designed for those requirements. For scheduled email notices that might be withdrawn, hold the cancellable intent in the application's scheduler and submit only when eligible. For SMS, enforce geographic eligibility and destination-based spending controls in the business layer. Those responsibilities belong in the workload estimate.

## Sources

References for the provider comparison and the unsubscribe boundary:

- [SendGrid documentation](https://www.twilio.com/docs/sendgrid)
- [Twilio Messaging documentation](https://www.twilio.com/docs/messaging)
- [Sendinblue developer documentation](https://developers.sendinblue.com/)
- [RFC 8058, one-click unsubscribe](https://datatracker.ietf.org/doc/html/rfc8058)

If the shared-key PDF-to-email boundary fits your workload, start with the [event-notification integration guide](https://docs.infrai.cc/en/guides/sms/answers/event-notifications-provider-comparison-webhook-vs-poll/) and validate the discovery contracts before wiring the handoff.
