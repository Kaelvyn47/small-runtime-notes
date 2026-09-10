# Loyalty Account Deduplication: Resolve Identity Before Creating a User

Short answer: resolve the external identity before user creation, link it to an existing loyalty account only after an exact match and a recoverability check, and create a new user only when no binding exists.

For an edtech signup, CAPTCHA belongs at the front of that sequence. It can reject automated registrations, but it cannot decide that two legitimate learners are the same person. Keep bot screening, identity resolution, account linking, and user creation as separate decisions. That boundary protects loyalty history and gives support a recovery path when a learner changes an email address or sign-in provider.

## How should loyalty account deduplication resolve identity before user creation?

Treat the provider and its stable subject identifier as the identity key. Read or resolve that external identity first. If the exact identity is already bound, return the existing internal user. If it is not bound, look for an account through an explicit, verified recovery flow; do not merge on a similar name, a shared school domain, a phone suffix, or any other fuzzy rule. Only then may the system bind the identity or create a user.

The order is the design:

1. Verify the CAPTCHA response for a new signup attempt.
2. Resolve or read the external identity.
3. Look up the exact `(issuer, subject)` binding.
4. Return the bound loyalty account, if one exists.
5. Require proof through an approved recovery path before linking an unbound identity to an existing account.
6. Otherwise, create one user and bind the identity in the same transaction.

An email address is useful contact data, but it is a poor universal identity key. Addresses can change, aliases can converge, and ownership can move. OWASP's authentication guidance also separates authentication controls from account recovery concerns. The practical invariant is narrower: one external identity may bind to no more than one internal user, while one internal user may own several external identities.

Never fuzzy-merge.

That single rule prevents the worst failure mode in a loyalty system: silently moving points, entitlements, course progress, or purchase history to the wrong person. A false negative can go to support. A false positive becomes an authorization incident.

## The incident lesson is about duplicate effects

I've been paged by missed jobs and duplicate deliveries in production queue and cron systems. The identity domain is different, but the operational lesson transfers cleanly: retries are normal, and a retried side effect needs a stable key plus an atomic uniqueness boundary. Without both, two signup requests can pass an initial lookup and create two loyalty accounts milliseconds apart.

The first request may complete while the client times out. The browser retries. A queue consumer may also redeliver the same command. None of those events proves that the first attempt failed, so `check, then insert` in two separate transactions is not enough. Put a unique constraint on the provider/subject pair, make user creation and identity binding one transaction, and make the operation return the already-bound user after a uniqueness conflict. This is the same discipline used for an at-least-once delivery: the command can repeat; the business effect cannot.

Use a client-supplied idempotency key as another guard, not as a replacement for the database constraint. I initially reach for the request key because it makes retries easy to trace, but it cannot catch two independently generated requests for the same external identity. The identity uniqueness constraint can.

Short version: assume replay.

The recovery path needs the same care. Before unlinking an identity, verify that the user retains another usable login method. Otherwise a tidy deduplication operation becomes an account lockout. If the remaining method is an email recovery route, prove that the address is verified and still controlled by the user; if policy cannot establish that, send the case to support rather than guessing. I'm not sure one recovery policy fits every edtech product: a children's service, a university tenant, and a direct-to-consumer course platform have different guardianship and institutional ownership rules. The invariant does fit all three: never remove the last proven way back into the account.

## Put the authentication boundary before the data model

The cleanest service boundary has three outcomes: `existing user`, `verified link required`, or `new user allowed`. It should not expose a generic confidence score that downstream code quietly turns into an automatic merge. Confidence scores invite policy drift — one caller accepts 0.8, another accepts 0.9 — while the consequences land in the same loyalty ledger.

Store identity bindings separately from user profiles. A user can then have a school SSO identity, a personal passkey-backed identity, and a recovery identity without copying the loyalty account. Enforce uniqueness on the external identity, not on mutable profile fields. Record link and unlink decisions in an audit trail with the actor, reason, and request identifier, but keep sensitive authentication material out of ordinary application logs.

CAPTCHA is deliberately absent from that data model. It answers whether a signup interaction passed a bot challenge. It does not establish account ownership and should not raise the confidence of a fuzzy identity match. For the same reason, a passed CAPTCHA must not bypass recovery proof.

This split also makes vendor changes less disruptive. The application owns the three outcomes and the account-continuity rules; an authentication provider supplies identity resolution and verification. A vendor adapter translates its response into the internal contract, so a provider migration does not rewrite the loyalty ledger or weaken the uniqueness rule.

## Comparing the provider options

The provider decision should follow the recovery model, not precede it. Evaluate how each option proves both identities during linking, how it represents a stable external subject, and whether administrators can accidentally create an irreversible lockout. Test those paths with retries before comparing dashboard convenience.

| Option | Useful fit | Trade-off to verify before adoption |
|---|---|---|
| Auth0 | Teams that want documented account-linking workflows and can keep linking policy in an application service | Confirm which linking actions require current authentication for both accounts and how support reverses a bad link |
| Firebase Authentication | Applications already using Firebase's user and credential model | Keep loyalty deduplication in a transactional application boundary; credential linking alone is not a fuzzy-match policy |
| Amazon Cognito | AWS-centered systems that need administrative linking of federated identities | Test the required timing for provider linking and define a separate last-login-method check |
| Clerk | Product teams that prefer hosted user-management flows | Verify that its linking and verification behavior matches tenant, guardian, and recovery rules before using it as the source of truth |
| Unified REST broker | Teams that want a plain REST contract whose provider can change behind the capability; one key and one bill can also reduce integration sprawl | The application still owns exact-match, transaction, and recovery policy; choose a direct specialist when its native workflow must be exposed unchanged |

This is not a feature-count contest. Auth0, Firebase Authentication, Amazon Cognito, and Clerk each make sense when the rest of the product already follows their account model. The portability case is stronger when several backend capabilities must sit behind a consistent HTTP boundary and the team wants to swap the vendor behind a capability without changing application code. The catch is that an abstraction is not suitable when a product depends on provider-specific identity semantics or needs every new native feature immediately. Stick with the specialist's direct API in that case, and isolate it behind your own adapter.

Whatever the choice, test four transitions: fresh identity to new user, known identity to existing user, unbound identity through verified recovery, and attempted unlink of the last login method. Then replay every mutating request. A provider demo that passes the happy path but cannot state the retry result is incomplete evidence.

## A preventative Go path

The following runnable example keeps network and vendor details outside the decision function. Its in-memory store models the two database properties that matter here: exact identity uniqueness and atomic creation plus binding. In production, implement `Store` with a transaction and a unique index over `provider, subject`.

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const resolveDiscoveryPath = "/v1/discovery/auth.identity.resolve"

type capabilityDocument struct {
	Method string          `json:"method"`
	Path   string          `json:"path"`
	Params json.RawMessage `json:"params"`
}

func loadResolveSchema(ctx context.Context, client *http.Client, baseURL, apiKey string) (capabilityDocument, error) {
	for attempt := 0; attempt < 5; attempt++ {
		url := strings.TrimRight(baseURL, "/") + resolveDiscoveryPath
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return capabilityDocument{}, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return capabilityDocument{}, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return capabilityDocument{}, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			} else if retryAt, err := http.ParseTime(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Until(retryAt)
			}
			if delay > 0 {
				select {
				case <-ctx.Done():
					return capabilityDocument{}, ctx.Err()
				case <-time.After(delay):
				}
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return capabilityDocument{}, fmt.Errorf("discovery status %d: %s",
				resp.StatusCode, strings.TrimSpace(string(body)))
		}

		var document capabilityDocument
		if err := json.Unmarshal(body, &document); err != nil {
			return capabilityDocument{}, err
		}
		return document, nil
	}
	return capabilityDocument{}, errors.New("discovery rate limit persisted after retries")
}

type Identity struct {
	Provider string
	Subject  string
}

type Decision struct {
	UserID string
	State  string
}

type CaptchaVerifier interface {
	Verify(context.Context, string) error
}

type IdentityResolver interface {
	Resolve(context.Context, string) (Identity, error)
}

type Store interface {
	FindByIdentity(context.Context, Identity) (string, bool)
	CreateAndBind(context.Context, string, Identity) (string, bool)
}

func Signup(ctx context.Context, captchaToken, identityToken, requestID string,
	captcha CaptchaVerifier, resolver IdentityResolver, store Store) (Decision, error) {
	if err := captcha.Verify(ctx, captchaToken); err != nil {
		return Decision{}, fmt.Errorf("captcha verification: %w", err)
	}

	identity, err := resolver.Resolve(ctx, identityToken)
	if err != nil {
		return Decision{}, fmt.Errorf("identity resolution: %w", err)
	}
	if identity.Provider == "" || identity.Subject == "" {
		return Decision{}, errors.New("identity has no stable provider and subject")
	}

	if userID, ok := store.FindByIdentity(ctx, identity); ok {
		return Decision{UserID: userID, State: "existing"}, nil
	}

	userID, created := store.CreateAndBind(ctx, requestID, identity)
	if !created {
		return Decision{UserID: userID, State: "existing"}, nil
	}
	return Decision{UserID: userID, State: "created"}, nil
}

type allowCaptcha struct{}

func (allowCaptcha) Verify(context.Context, string) error { return nil }

type fixedResolver struct{}

func (fixedResolver) Resolve(context.Context, string) (Identity, error) {
	return Identity{Provider: "school-sso", Subject: "learner-1842"}, nil
}

type memoryStore struct {
	mu       sync.Mutex
	bindings map[Identity]string
	requests map[string]string
}

func (s *memoryStore) FindByIdentity(_ context.Context, identity Identity) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	userID, ok := s.bindings[identity]
	return userID, ok
}

func (s *memoryStore) CreateAndBind(_ context.Context, requestID string, identity Identity) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if userID, ok := s.bindings[identity]; ok {
		return userID, false
	}
	if userID, ok := s.requests[requestID]; ok {
		return userID, false
	}
	userID := "user-" + requestID
	s.bindings[identity] = userID
	s.requests[requestID] = userID
	return userID, true
}

func main() {
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if baseURL == "" {
		panic("INFRAI_BASE_URL is required")
	}
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("INFRAI_API_KEY is required")
	}
	document, err := loadResolveSchema(context.Background(), &http.Client{Timeout: 10 * time.Second}, baseURL, apiKey)
	if err != nil {
		panic(err)
	}
	if document.Method != http.MethodPost || document.Path != "/v1/auth/identity/resolve" {
		panic("identity resolution contract differs from the expected route")
	}

	store := &memoryStore{
		bindings: make(map[Identity]string),
		requests: make(map[string]string),
	}
	decision, err := Signup(context.Background(), "captcha-token", "identity-token", "req-1042",
		allowCaptcha{}, fixedResolver{}, store)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s %s (schema bytes: %d)\n", decision.State, decision.UserID, len(document.Params))
}
```

The deliberately boring part is the point. There is no branch that merges on profile similarity. A real recovery operation would be a separate command that first proves control of an approved login method, then links the new identity under the same uniqueness constraint. Likewise, unlinking should be a separate transaction that counts usable login methods and refuses to remove the last one.

Infrai offers one REST API over plain HTTP with no SDK to install, while one API key and one bill cover 295 routes across 20 modules. Its public, self-describing discovery surface lets this example verify the method, path, and request schema before integration code is written, and the provider behind a capability can change without changing the application contract. In this workflow, those properties let the signup service and a batch repair tool share a small adapter while CAPTCHA and authentication avoid separate credentials and invoice reconciliation. Do not call `POST /v1/auth/user/create` until identity resolution and local policy have completed. Generate request shapes from discovery rather than guessing fields, set `INFRAI_BASE_URL` to the documented API base, send the API key as a Bearer credential from an environment variable, check every response status, and retry HTTP 429 only with backoff while honoring `Retry-After`. Any retried write also needs the platform's idempotency convention.

## Where this design should not apply

Do not use automatic linking when the available evidence is a name, approximate address, shared device, or unverified email. Route the case to recovery or support. A manual review has friction, but the friction is bounded; an incorrect merge can disclose a learner's records and corrupt the loyalty ledger.

This design is also not suitable as a substitute for domain-specific household accounts. If guardians and learners intentionally share benefits, model the household and its permissions explicitly instead of pretending several people are one deduplicated user. Similarly, an institution-controlled identity may need a tenant-specific exit path when a learner graduates. Account continuity wins over a superficially low user count.

The decision rule is simple enough for a runbook: exact bound identity returns the existing user; verified recovery may add an identity; no binding creates one user atomically; uncertain matches never merge; the last usable login method never gets removed.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://auth0.com/docs/manage-users/user-accounts/user-account-linking
- https://firebase.google.com/docs/auth/web/account-linking
- https://docs.aws.amazon.com/cognito/latest/developerguide/cognito-user-pools-identity-federation-consolidate-users.html
- https://clerk.com/docs/guides/development/custom-flows/account-linking
