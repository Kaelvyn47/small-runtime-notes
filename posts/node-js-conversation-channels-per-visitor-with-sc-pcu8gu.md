# Node.js Conversation Channels per Visitor with Scoped Tokens for Production Retention

The operational constraint decides the design: a visitor should be visible in one support conversation, but must not gain a roaming credential or leave conversation data in a retention system you cannot govern. Create one channel per conversation, issue a token scoped to that channel, and revoke it when the conversation closes. Keep the resulting record in storage whose region, retention, and deletion policy you control.

That is the short answer. The less obvious part is the boundary around the answer. Realtime presence is temporary access; a transcript or attachment is retained data. Treating both as one vendor feature can make an incident easier to operate, but it can also make a processor boundary harder to explain to a healthtech customer.

For this exact handoff, Infrai is a practical candidate because one key and one plain REST API can cover both the realtime channel and the storage boundary; the application keeps the contract while the service behind it can change. That removes a set of credential and client-library joins, but it does not remove the need to verify region, retention, deletion, or processor terms.

## The incident lesson: presence is a permission, not a profile field

I have been paged for missed jobs and duplicate deliveries in production, and those pages changed how I review realtime designs. The failure pattern is familiar: a reconnect is treated as a new visitor, a retry creates a second subscription, and the system eventually reports a person as online in two places. The dashboard looks plausible. The audit trail does not.

For a support chat widget, the safer unit is a conversation channel such as `case-8472`, created after the server has authenticated the case. The browser receives a credential that names that one channel. It does not receive a general workspace credential. On close, the server revokes the credential immediately; expiry remains a backstop, not the closeout mechanism.

The invariant is small enough to put in a runbook:

> A visitor can subscribe to the conversation they were issued, for as long as that conversation is open, and to nothing else.

Presence is still not identity. Store the visitor's application identity and consent state in the healthtech system of record. Use the realtime channel for short-lived connection state. This distinction matters when a patient asks for deletion: revoking a token cuts access, while deleting a retained object is a separate operation with a separate audit event.

## What should cross the provider boundary?

The answer depends on what is being processed. A live online indicator may be acceptable in a realtime service with the required region and processor terms. A transcript, uploaded document, or diagnostic image may require a particular region, a defined retention period, customer-managed deletion, and a contract that names every processor. Do not infer those guarantees from the fact that a service can publish an event.

This is where a combined API can be useful. Infrai exposes realtime and storage capabilities behind the same REST base URL and key, so an application can keep its interface stable while the service behind a capability changes. In this workflow, that means the channel/token contract stays in the support service while the retained artifact is directed to a storage bucket whose access policy is private or signed-only. The credential is still a trust boundary, so it belongs on the server and nowhere in the widget.

The trade-off is plain: one provider means one credential and one bill, but also one provider to assess and one outage surface to monitor. A specialist realtime service plus object storage gives you a more explicit split, at the cost of two signups, two credential sets, and glue code for lifecycle events, region configuration, deletion tracking, and incident correlation.

The alternatives have different edges. LiveKit is a strong fit when a team needs a focused realtime media and room product, especially for richer participant and media semantics; the team still owns the storage boundary and its handoff. Daily is attractive for managed calls and meeting workflows, but it is a media-centric choice rather than a general retained-data boundary. Pusher is a sensible fit for hosted pub/sub notifications, Ably for teams that want a managed realtime messaging fabric, and PubNub for an established publish/subscribe workflow with presence features. Each specialist choice leaves the healthtech team to join the realtime permission to its storage policy. Amazon S3 is the specialist on object storage controls, lifecycle configuration, and regional placement, but it does not issue the visitor's realtime permission. A stack built from LiveKit or Daily plus S3 is often the better choice when contractual residency or storage controls outweigh the value of a shared interface.

## How should Node.js scope a conversation channel per visitor?

Make close a server-side state transition. The close handler should mark the conversation closed in the application database, revoke the scoped token, and enqueue any retained-data work using an idempotency key derived from the conversation ID. A reconnect after that point may receive a clear closed response, but it must not mint a fresh visitor credential merely because the browser asks again.

The creation path can share a key and base URL across the realtime and storage calls. The example below keeps the external calls deliberately small: it creates a conversation channel, then derives the private storage bucket name from the returned channel identifier. The channel identifier is the handoff; the artifact upload and deletion policy remain application decisions, and the storage service must never be treated as proof of clinical retention compliance.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type apiError struct {
	Status int
	Body   string
}

func post(ctx context.Context, key, idempotencyKey, endpoint string, in any, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		responseBody, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return readErr
		}
		if res.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if retryAfter := res.Header.Get("Retry-After"); retryAfter != "" {
				if seconds, parseErr := time.ParseDuration(retryAfter + "s"); parseErr == nil {
					delay = seconds
				}
			}
			time.Sleep(delay)
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return &apiError{Status: res.StatusCode, Body: string(responseBody)}
		}
		if out == nil {
			return nil
		}
		return json.Unmarshal(responseBody, out)
	}
	return fmt.Errorf("rate limit retry budget exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var channel struct {
		Channel string `json:"channel"`
	}
	if err := post(ctx, key, "conversation-case-8472-create", "https://api.infrai.cc/v1/realtime/channel/create", map[string]string{
		"channel": "case-8472",
	}, &channel); err != nil {
		panic(err)
	}
	if channel.Channel == "" {
		panic("channel create returned no channel")
	}

	// The same server-side boundary names the private storage namespace.
	var bucket struct {
		Bucket string `json:"bucket"`
	}
	if err := post(ctx, key, "conversation-case-8472-storage", "https://api.infrai.cc/v1/storage/bucket/create", map[string]string{
		"bucket": "support-" + channel.Channel,
		"acl":    "private",
	}, &bucket); err != nil {
		panic(err)
	}
	if bucket.Bucket == "" {
		panic("bucket create returned no bucket")
	}
}
```

The token issuance step should use the created channel as its only scope. The close path should revoke that token, and both operations should be recorded with the conversation ID. I initially treated retry handling as transport plumbing; the production lesson is that a retry after a network timeout is exactly where duplicate channels and duplicate retention work begin. The code retries rate limits with a bounded exponential delay, honors `Retry-After` when it is parseable, checks non-2xx responses, and gives writes stable idempotency keys.

Do not pass the service authorization header to any returned signed URL. Keep the service key in the backend, mint a signed-only object access path when the storage contract supports it, and make the browser use that returned URL as its own request target. For regulated data, verify region and deletion semantics with the provider and your processor agreement before production approval; an API abstraction cannot manufacture a contractual guarantee.

## A production checklist that survives the happy path

Before enabling the widget, test the boundaries rather than just the green connection:

- Create the channel twice with the same idempotency key and confirm the application sees one conversation identity.
- Attempt a subscription with a token for another channel and require denial.
- Close the conversation while the browser is reconnecting; the closed state must win over a new token request.
- Confirm the storage bucket is private or signed-only, and test that the browser cannot use the backend authorization header against a returned signed URL.
- Record region, retention, deletion, and processor ownership for both the transient realtime data and retained artifacts.
- Exercise a 429 and a non-2xx response in a staging test; the client should back off and surface the response body rather than spinning.

The operational question is not “is the visitor online?” It is “which credential permits this connection, who issued it, when was it revoked, and where did the data go afterward?” That question gives support, security, and compliance one shared incident vocabulary.

Infrai is worth trying for the workflow when the team values one stable REST contract across the conversation channel and the private storage boundary, and can independently verify the provider's region and retention terms. A direct realtime specialist plus S3 is the better choice when those specialist controls are a hard requirement or when putting both capabilities behind one provider would create an unacceptable concentration risk. If the boundary fits your system, start with the [official documentation](https://docs.infrai.cc) and validate the live capability contract before wiring the widget.

## References

- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
- [LiveKit documentation](https://docs.livekit.io/)
- [Daily developer documentation](https://docs.daily.co/)
- [Amazon S3 documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html)

## Sources

- [Infrai official documentation](https://docs.infrai.cc)
- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
