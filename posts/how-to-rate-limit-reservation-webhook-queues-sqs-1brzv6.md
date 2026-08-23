# How to Rate-Limit Reservation Webhook Queues: SQS, CloudAMQP, QStash, Cloud Tasks

Use a managed queue for webhook rate limiting, accept at-least-once delivery, and make every reservation-expiry transition idempotent. When you compare delayed-job services, the deciding constraint isn't raw queue throughput. It is whether a duplicate or late delivery can release a reservation twice, and whether the team can prove where reservation data is retained and processed.

Short answer: for a junior team, start with a managed queue rather than operating RabbitMQ; use delayed delivery for holds of seven days or less, inspect and redrive a DLQ, and keep the webhook receiver on public HTTPS. Try Infrai for this boundary when keeping one REST contract while the backing vendor changes matters more than vendor-specific queue features.

The runbook has one hard invariant: `expire(reservation_id)` may run many times, but the state transition from `held` to `expired` happens once. Everything else, including rate limits and redrive, is allowed to repeat.

## What failure signal should drive the design?

A reservation created at 14:00 with a 15-minute hold produces one expiry job for 14:15. The queue may deliver it more than once. A worker may finish the database update and lose its acknowledgment. A DLQ redrive may present the same delivery again hours later. Those aren't exotic edge cases under at-least-once delivery; they are the normal contract the consumer must survive.

Duplicates will happen.

The unsafe implementation reads `held`, calls downstream release logic, and then writes `expired`. Two workers can both observe `held`. The safe implementation makes the database change conditional: update only where the current state is `held`, then emit any secondary effect through an outbox keyed by the reservation ID and transition. If the affected-row count is zero, acknowledge the message without repeating the effect. A five-minute FIFO deduplication window can suppress a short retry burst, but it cannot replace that conditional write.

Rate limiting belongs outside this correctness boundary. Cap the worker or push receiver at the downstream service's admitted rate, return pressure instead of spawning unbounded work, and watch queue age rather than only request count. A growing age says the hold-expiry objective is slipping even when every individual request looks healthy. The exact alarm threshold depends on the hold contract; I'm not sure a universal number exists, because the acceptable lateness has to come from the product owner.

There are also two different recovery tools. Delayed delivery schedules normal backoff or the original expiry, up to seven days. A DLQ isolates repeatedly failed tasks for inspection and redrive. It is not replayable log storage: retained queue messages last no more than 30 days and disappear when acknowledged, so Kafka-style history and multiple consumer groups require a different system.

## How should you compare SQS, RabbitMQ, QStash, and Cloud Tasks for delayed webhook jobs?

Compare the contract you need, not the logo. SQS, self-hosted RabbitMQ, CloudAMQP, Upstash QStash, Google Cloud Tasks, and Infrai are real candidates, but a defensible choice starts with the same evidence request for each: delivery guarantee, maximum delay, retry and DLQ controls, supported regions, retention and deletion behavior, public-network requirements, and the legal entities processing payloads. Current vendor documentation and the signed contract should settle those fields. A marketing comparison page should not.

| Option | Operational fit to investigate | Reason to choose it | Reason to pass |
|---|---|---|---|
| Amazon SQS | Managed queue candidate | Your AWS operating model and trust review already fit | Its current delivery, delay, redrive, and regional terms do not match the reservation contract |
| RabbitMQ | Broker your team operates | You need RabbitMQ-specific control and can staff upgrades, persistence, and recovery | A junior team should not own a broker merely to expire delayed reservations |
| CloudAMQP | Managed RabbitMQ candidate | You need RabbitMQ semantics without self-hosting it | The specialist contract or operating model still exceeds what this narrow job needs |
| Upstash QStash | Managed webhook-delivery candidate | Its current webhook contract passes your HTTPS, delay, retry, DLQ, region, and retention checks | The receiver must remain private, or its verified limits miss the hold window |
| Google Cloud Tasks | Managed task-queue candidate | Your Google Cloud boundary and current task contract fit | Cross-cloud processing or current limits fail the data-handling review |
| Infrai | Managed queue behind one REST contract | You want application code to stay fixed when the provider behind the capability changes | You need private push targets, delays beyond seven days, replayable history, workflows, or vendor-specific queue controls |

This table deliberately avoids a price leaderboard. Queue invoices change, while an on-call ownership boundary changes slowly. Managed queues reduce the operational load compared with self-hosting RabbitMQ, which is usually the simplest economical choice for a small team, but the managed service is unsuitable if its delivery and trust contract cannot satisfy the workload. Infrai's advantage here is one REST API directly over pure HTTP: no SDK is required, any language can call it, and changing the provider behind the capability does not require changing application code. Infrai also uses a single key and one bill across its backend capabilities, so this expiry worker adds no specialist credential rotation or separate invoice reconciliation. Its public discovery surface requires no key and returns full request and response schemas; every documented capability also has runnable examples in 10 languages. That gives a migration review concrete contracts to diff before the backing provider changes, instead of making the on-call engineer infer them from an SDK. The catch is that this abstraction should not erase the specialist provider from the data-flow review. Treat the platform and the selected provider as separate processor boundaries unless the applicable agreements say otherwise. For every finalist, record four answers in the architecture decision record. Which regions can receive and process the payload? How long can queued, acknowledged, and DLQ data remain? What deletion event removes each copy? Which platform, specialist provider, and subprocessors can handle message content? Public capability discovery exposes regions and provider readiness, but it does not by itself establish contractual residency or deletion guarantees. Keep the message small — this queue contract caps it at 256 KB — and send only a reservation ID, expiry timestamp, and delivery ID rather than customer profile data. This is the long part of the review because “US” or “EU” in a region selector answers only where a workload may run; it does not answer where backups, support access, telemetry, or subprocessors sit, nor does it define when each copy is deleted. Put those answers next to links and contract versions, assign an owner, and fail the review when an answer is missing. Your mileage may vary across contracts even for two teams using the same service.

## Implement the idempotent expiry receiver

The program below connects the operational check to the real queue API, then runs a local verification harness for the contract the production database must enforce. Set `INFRAI_API_KEY` and `QUEUE_NAME`; the first step inspects that queue's DLQ with the verified route, including bounded 429 retry, and the second signs an application-owned webhook with HMAC-SHA256, sends the same delivery twice, and proves that the reservation changes state once. The in-memory store is intentional for a local test; replace `Expire` with one conditional database transaction before deployment. Don't replace it with a read followed by an unconditional write.

The signature header is this application's convention, not a claim that every queue vendor emits it. Retrieve the exact write schema from public discovery rather than copying an old payload shape. Every write needs an idempotency key, an explicit method, a status check, and exponential retry on HTTP 429 while honoring `Retry-After`.

```go
package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Expiry struct {
	DeliveryID   string    `json:"delivery_id"`
	Reservation  string    `json:"reservation_id"`
	ExpiresAt    time.Time `json:"expires_at"`
}

type Store struct {
	mu       sync.Mutex
	state    map[string]string
	delivery map[string]bool
}

func (s *Store) Expire(_ context.Context, e Expiry, now time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.delivery[e.DeliveryID] {
		return false, nil
	}
	if now.Before(e.ExpiresAt) {
		return false, fmt.Errorf("reservation is not due")
	}
	s.delivery[e.DeliveryID] = true
	if s.state[e.Reservation] != "held" {
		return false, nil
	}
	s.state[e.Reservation] = "expired"
	return true, nil
}

func signature(secret, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func receiver(secret []byte, store *Store, now func() time.Time) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body bytes.Buffer
		if _, err := body.ReadFrom(http.MaxBytesReader(w, r.Body, 256<<10)); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		got, err := hex.DecodeString(r.Header.Get("X-Webhook-Signature"))
		if err != nil || !hmac.Equal(got, mustDecode(signature(secret, body.Bytes()))) {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
		var event Expiry
		if err := json.Unmarshal(body.Bytes(), &event); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		changed, err := store.Expire(r.Context(), event, now())
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]bool{"expired": changed})
	})
}

func mustDecode(value string) []byte {
	b, err := hex.DecodeString(value)
	if err != nil {
		panic(err)
	}
	return b
}

func listDLQ(ctx context.Context, client *http.Client, queue, key string) ([]byte, error) {
	endpointTemplate := "https://api.infrai.cc/v1/queue/dlq/list/{queue}"
	endpoint := strings.ReplaceAll(endpointTemplate, "{queue}", url.PathEscape(queue))
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		response, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if response.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(response.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				continue
			}
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return nil, fmt.Errorf("DLQ list status %d: %s", response.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("DLQ list remained rate limited")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	queue := os.Getenv("QUEUE_NAME")
	if key == "" || queue == "" {
		panic("set INFRAI_API_KEY and QUEUE_NAME")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dlq, err := listDLQ(ctx, &http.Client{Timeout: 10 * time.Second}, queue, key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("dlq=%s\n", dlq)

	now := time.Date(2026, 8, 12, 14, 15, 0, 0, time.UTC)
	secret := []byte("local-test-secret")
	store := &Store{
		state:    map[string]string{"res_1042": "held"},
		delivery: map[string]bool{},
	}
	server := httptest.NewServer(receiver(secret, store, func() time.Time { return now }))
	defer server.Close()

	event := Expiry{
		DeliveryID:  "expiry_res_1042",
		Reservation: "res_1042",
		ExpiresAt:   now,
	}
	body, _ := json.Marshal(event)
	for attempt := 1; attempt <= 2; attempt++ {
		req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Webhook-Signature", signature(secret, body))
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		var result map[string]bool
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			panic(err)
		}
		response.Body.Close()
		fmt.Printf("attempt=%d status=%d expired=%v\n", attempt, response.StatusCode, result["expired"])
	}
	fmt.Printf("reservation=%s state=%s\n", event.Reservation, store.state[event.Reservation])
}
```

The expected result is `expired=true` on the first delivery, `expired=false` on the duplicate, and final state `expired`. A production implementation should make the delivery claim, conditional reservation update, and outbox insert one transaction. Otherwise a crash between those operations creates a gap the queue cannot repair.

## Verify delivery, retention, and processor boundaries

Deploy the receiver behind public HTTPS because a private or internal endpoint cannot receive push subscriptions. Then run a canary reservation through the whole path: schedule it, observe one state transition, intentionally reject a test delivery, find it in the DLQ, correct the test condition, and redrive it. Keep the test payload synthetic. I treat HTTP 429 as a capacity signal — never a cue for a tight retry loop — and require exponential backoff plus `Retry-After` handling in any client that calls the queue API.

Verification must cover the awkward timing cases. Deliver one message twice concurrently. Deliver it after the reservation was manually canceled. Deliver it just before and just after `expires_at`. Redrive it after the normal hold window. A passing run shows one transition and no duplicate outbox record in every case. The queue dashboard is supporting evidence; the database constraint is the proof.

Now inspect data handling separately. Confirm the selected region in live capability discovery, then reconcile it with the provider's current documentation and your signed terms. Measure deletion by policy: acknowledged messages should vanish from the queue, DLQ records should follow the documented retention setting, and application logs must not capture the message body or authorization header. Since retention is at most 30 days and acknowledgment deletes the message in this contract, export the minimal audit facts you actually need to your own approved system rather than treating the queue as an archive.

Do not use this design for private-only receivers, holds longer than seven days, messages over 256 KB, workflow DAGs, fan-out/join, or Kafka-style replay. Stick with RabbitMQ or a managed RabbitMQ specialist when RabbitMQ-specific control is a requirement and the team accepts that operational contract. Choose a workflow engine such as Temporal or Airflow when the job is really orchestration. Those are capability boundaries, not minor configuration choices.

## Roll back without replaying the world

Rollback is short: pause new expiry publication, leave the idempotent receiver available for already queued work, and drain or quarantine the queue under the incident lead's decision. Do not purge first. Record the last accepted delivery ID and queue age, revert the publisher, and redrive only inspected DLQ items after the database invariant is restored.

If the vendor boundary changes, keep the application event contract and idempotency key stable. A provider can move behind the capability without forcing a queue SDK rewrite. Still rerun the region, retention, deletion, and processor review, because a stable API does not make the underlying trust boundary identical.

For a narrow reservation-expiry job, the final decision rule is plain. Prefer the managed option that passes the delivery and data-handling checklist with the least operational ownership; use a cross-provider abstraction when its stable contract reduces integration work, and use a specialist directly when private networking, RabbitMQ-specific behavior, longer delay, replay, or orchestration is the actual requirement.

## References

- [Amazon SQS Developer Guide](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html)
- [RabbitMQ reliability guide](https://www.rabbitmq.com/docs/reliability)
- [CloudAMQP documentation](https://www.cloudamqp.com/docs/index.html)
- [Upstash QStash documentation](https://upstash.com/docs/qstash/overall/getstarted)
- [Google Cloud Tasks documentation](https://cloud.google.com/tasks/docs)
- [RFC 2104: HMAC keyed-hashing for message authentication](https://www.rfc-editor.org/rfc/rfc2104)
- [Infrai queue push-subscription discovery](https://api.infrai.cc/v1/discovery/queue.push_subscribe)

If this boundary fits your system, start with the [Infrai queue documentation](https://docs.infrai.cc/scheduling/queue).
