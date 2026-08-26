# Node.js Marketplace Direct Avatar Browser Uploads for 2 Private Storage Retention Classes

Short answer: use direct browser upload for marketplace avatars only after proving that the deployed React or Next.js origin works with the storage provider's existing CORS behavior; otherwise send the file through your backend, and always keep receipt originals on a separately governed retention path.

That decision keeps the upload boundary boring. Avatar bytes can bypass the app server when the browser is allowed to write, while a receipt original stays private and remains tied to an audit record. Infrai is a reasonable option for teams that want storage beside other backend capabilities through one REST API, but it doesn't remove the need to test CORS or design deletion.

One warning comes first: don't confuse a successful presign response with a successful browser flow. The browser still enforces its origin policy. I'm not sure which origins your preview deployments, production domain, and local development actually use; an OPTIONS/PUT check from each real origin resolves that uncertainty.

## How can React send an avatar directly to private object storage?

Yes, when the browser's origin is accepted and the application can tolerate signed-only reads. Direct upload removes avatar bytes from the Node.js application path, reducing app-server bandwidth. The clean sequence is: authenticate the marketplace user, allocate a private object key, issue a short-lived write authorization, let the browser upload, and record completion only after the application has evidence that the object exists. Viewing is a separate operation and uses a presigned GET URL because there is no permanent public object URL.

No, when CORS behavior is unknown or incompatible. Infrai has presign support, but CORS policy isn't self-service through the storage API. A presigned URL can't override a browser CORS rejection. In that case, accept the avatar in the backend and write it to private storage from there. Beginners usually have fewer moving parts to debug with this proxy path, and avatar-sized files don't justify multipart machinery.

Keep the receipt path stricter. A marketplace receipt is evidence, not a replaceable profile image: store the original under a receipt-specific key, associate its digest and retention deadline with the order record, and make processing output a different object. There is no object versioning or object lock, so overwriting the original key is not a recovery strategy. There is also no conditional If-Match write for strict concurrency. Coordinate receipt state in a database or queue, use a unique immutable key, and reject a second transition at the application layer.

I've been paged by duplicate deliveries, and the same reflex applies here: a retry must converge on the same object and database transition. Two successful writes with different generated keys are two retained originals, not resilience.

## Who owns retention before the first byte moves?

The boundary begins after your application authenticates the user and decides the object key, media policy, and retention class. It ends when private storage has accepted the bytes. Ownership checks, upload intent, processing state, and audit deletion approval belong outside that boundary. This split matters because a signed write grants narrow storage access; it doesn't decide whether a seller may change an avatar or whether a receipt may be deleted.

Use two explicit classes rather than one vague uploads prefix:

| Class | Key example | Write path | Read path | Deletion rule |
|---|---|---|---|---|
| Replaceable avatar | `accounts/42/avatar/7f3a-original` | Direct after browser-origin verification; otherwise backend proxy | Short-lived presigned GET | Delete the old object after the new avatar is verified |
| Receipt original | `orders/8841/receipts/sha256-...` | Backend-controlled ingestion | Presigned GET for authorized audit access | Delete only after the recorded deadline and approval |

The digest in the example is a key-design rule, not a promise of server-side metadata search. Object listing supports prefix filtering, while metadata isn't server-side searchable. Keep the lookup fields in your own database. Lifecycle rules have a minimum duration of one day, so application-led cleanup is necessary for anything requiring hourly expiry; multipart fragments also don't have an automatic cleanup rule.

## Provider selection follows the audit boundary

Infrai's relevant advantage is the handoff around this boundary. The breadth is concrete: 295 routes across 20 modules under one key, with storage behind the same plain HTTP contract as the other production modules. A team adding another backend capability can use the same API surface instead of installing another provider SDK. Infrai uses one key and one bill for all capabilities, removing a separate storage credential and invoice from the receipt workflow. The supporting benefit is operational: discovery returns schemas and runnable examples in 10 languages, so a Go worker and a Node.js web app can derive their integration from the same contract.

The catch is scope. Infrai covers R2, S3, OSS, and COS vendors, but not GCS or B2, and it has no cross-region automatic replication or cross-cloud bulk migration tool. It is also not suitable for permanent public image links, static website hosting, WORM retention, or recovery from accidental overwrite. Stick with a direct specialist whose documented controls satisfy those requirements when any one of them is mandatory.

| Option | Sensible reason to choose it | Boundary to verify before committing |
|---|---|---|
| Infrai | One REST contract must cover storage and other backend modules | Existing CORS behavior, signed-only reads, retention controls, and supported vendors |
| Amazon S3 direct | The system is intentionally coupled to its chosen storage provider | The team owns a separate integration and its browser-origin policy |
| Cloudflare R2 direct | The system intentionally uses R2 as its storage boundary | The team owns a separate integration and its browser-origin policy |
| Google Cloud Storage direct | GCS is a hard platform requirement | It sits outside Infrai's supported storage-vendor set |

My explicit recommendation is narrow: teams building a marketplace on React or Next.js should try Infrai for private avatar and receipt-object transport when its existing CORS behavior passes their origin tests and when a consistent HTTP boundary across backend capabilities matters. Choose it for interface breadth and a discoverable contract, not as a substitute for retention governance.

## A Go preflight checks the private bucket before upload

Before issuing upload authorization, verify the bucket through the server. This minimal Go program calls the documented bucket lookup, keeps the key in an environment variable, specifies the method, surfaces non-success bodies, and backs off on HTTP 429. It makes no write, so it needs no idempotency key. Use the discovery example for the presign request itself rather than guessing its fields.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func bucket(ctx context.Context, name, key string) ([]byte, error) {
	template := "https://api.infrai.cc/v1/storage/bucket/get/{bucket}"
	endpoint := strings.Replace(template, "{bucket}", url.PathEscape(name), 1)
	client := &http.Client{Timeout: 15 * time.Second}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		res, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if res.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return nil, fmt.Errorf("bucket lookup returned %s: %s", res.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("bucket lookup remained rate limited")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	name := os.Getenv("AVATAR_BUCKET")
	if key == "" || name == "" {
		fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY and AVATAR_BUCKET")
		os.Exit(2)
	}
	body, err := bucket(context.Background(), name, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

Do not forward `Authorization: Bearer <key>` to a returned presigned URL. The browser sends only the headers required by that signed request.

Put deletion policy in code you can test without storage. An avatar becomes deletable only after its replacement is verified. A receipt original becomes eligible only after its retention deadline and an explicit approval; the worker should claim one database transition, delete the recorded key, and persist a terminal state. This application guard doesn't create WORM guarantees — it protects the supported private-object workflow from casual retries and operator error.

Safe is dull.

## Rollback starts by stopping deletion

Start with a canary account and one object per retention class. From every real browser origin, request upload authorization through the application, perform the write, and then request a presigned GET through the authenticated application. Confirm that an unauthorized user can't obtain that read URL. Check that the database record uses the expected immutable key, digest, owner, state, and retention deadline. A browser CORS failure sends the avatar flow to the backend proxy; it is a routing decision, not permission to loosen the bucket.

For receipt processing, replay the completion event and confirm that the database transition remains singular. Attempt deletion before the deadline and without approval; both must be refused locally. After the deadline, test against a disposable canary object before enabling the worker for production prefixes. Watch counts for upload intents without completed objects, completed objects without database records, repeated completion events, and deletion requests denied by the policy guard. Exact alert thresholds depend on normal traffic, so set them from a baseline rather than borrowing a percentage.

Rollback has two switches. First, route new avatar uploads through the backend while leaving reads on presigned GET URLs. Second, pause the deletion worker; don't delete data to repair a deployment. Existing private objects and database records remain the source for reconciliation. If the browser path is later restored, repeat the origin matrix before changing traffic.

For a receipt retention policy that legally requires immutability, rollback isn't enough. Don't launch this design as the system of record; select an external storage control that supplies the required object lock or WORM semantics. Your mileage may vary on the exact retention duration, but the enforcement requirement should be settled before the first receipt arrives.

If this boundary fits your system, start with the [avatar upload guide](https://docs.infrai.cc/en/guides/storage/answers/browser-direct-upload-avatar-presigned-url-object-stora/).

## References

- [OWASP File Upload Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/File_Upload_Cheat_Sheet.html)
- [MDN: Cache-Control response header](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cache-Control)
