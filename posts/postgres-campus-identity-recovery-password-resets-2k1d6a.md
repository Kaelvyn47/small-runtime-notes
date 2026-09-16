# Postgres Campus Identity Recovery: Password Resets Resistant to User Enumeration

A recovery endpoint has to treat queue delivery as an unreliable transport event, not as evidence that a student exists. The right choice for an education platform is an opaque, single-use password-reset token backed by a transactional state change, a uniform public response, and layered abuse controls; after a successful reset, revoke the student's existing sessions and rotate any refresh-token family that remains authorized.

That choice is less convenient than exposing “email not found,” and it creates support work when students mistype an address. It also closes the enumeration channel that matters most in a campus setting: a bot comparing status, body, timing, and downstream behavior across a directory-sized list of student identifiers.

I've been paged by missed jobs and duplicate deliveries. The lesson carries directly into account recovery — an email worker can run twice, run late, or lose its final acknowledgement, while the security decision still has to happen exactly once. A duplicated message must not create two independently valid reset capabilities. A delayed message must not revive an expired one. Keep that invariant in the database, where workers can contend safely, rather than in the mail queue.

## How should a student account recovery flow prevent password reset enumeration?

Return the same public status and generic message for every syntactically valid request, regardless of whether the account exists. OWASP explicitly recommends a consistent message and consistent response time, and it warns that asynchronous processing can help prevent timing differences. “If an eligible account matches, we’ll send instructions” is accurate without confirming enrollment.

Use the same envelope, too. A `200 OK` with one JSON shape is easy for clients; a `202 Accepted` can also be defensible if the endpoint genuinely queues work, but RFC 9110 says acceptance does not mean processing has completed. Pick one contract and keep it identical for known accounts, unknown accounts, inactive accounts, and addresses blocked by internal policy. Don't return `404` for one branch, change response length, set a special header, or make the known-account path wait on an email provider. The server still needs different internal outcomes: record a private result such as `accepted_known`, `accepted_unknown`, or `rate_limited`, but never echo it to the caller. Unknown-address requests can take a comparable bounded code path without manufacturing an account or sending mail. Exact latency parity is hard under real load, so I wouldn't promise it; a distribution test across many known and unknown samples is more useful than comparing two requests with a stopwatch. Enumeration resistance also extends beyond the HTTP response. Per-account throttling that only activates for real users leaks existence through retry behavior. A CAPTCHA shown only after a known address leaks it visually. Even a mailbox-side difference can help an attacker who controls candidate inboxes. Apply coarse IP and network controls before lookup, then enforce private account-scoped limits after lookup while preserving the same external response. Monitor the private distinction; publish none of it.

Same outside. Different inside.

## The invariant belongs in Postgres, not the email queue

A reset request should create one opaque token, store only a cryptographic digest, associate it with a purpose and account, and give it a short expiry. OWASP calls for reset tokens that are randomly generated, sufficiently long, securely stored, single use, and expiring. The raw token goes into the email link; logs, analytics, and database rows should never contain it.

The important transition is `issued -> consumed`. It must be conditional and atomic. In Postgres, a transaction can lock the matching row, reject an expired or previously consumed token, update the password verifier, mark the token consumed, and advance the account's session epoch before commit. If two browser tabs submit the same link, one commits and the other gets the same private invalid-token result. No race window remains between “checked” and “used.”

The following Go sketch shows the preventative path. Its generic interfaces keep the transaction boundary visible; password hashing and token digest construction belong in separately tested implementations.

```go
package recovery

import (
	"context"
	"errors"
	"time"
)

var ErrInvalidToken = errors.New("invalid recovery token")

type TokenRow struct {
	AccountID string
	ExpiresAt time.Time
	Consumed  bool
}

type Tx interface {
	LockResetToken(ctx context.Context, digest []byte) (TokenRow, error)
	ReplacePassword(ctx context.Context, accountID string, verifier []byte) error
	ConsumeResetToken(ctx context.Context, digest []byte, usedAt time.Time) error
	RevokeSessions(ctx context.Context, accountID string, revokedAt time.Time) error
	Commit() error
	Rollback() error
}

type Store interface {
	Begin(ctx context.Context) (Tx, error)
}

func Consume(ctx context.Context, store Store, digest, verifier []byte, now time.Time) error {
	tx, err := store.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	row, err := tx.LockResetToken(ctx, digest)
	if err != nil || row.Consumed || !now.Before(row.ExpiresAt) {
		return ErrInvalidToken
	}
	if err := tx.ReplacePassword(ctx, row.AccountID, verifier); err != nil {
		return err
	}
	if err := tx.ConsumeResetToken(ctx, digest, now); err != nil {
		return err
	}
	if err := tx.RevokeSessions(ctx, row.AccountID, now); err != nil {
		return err
	}
	return tx.Commit()
}
```

Keep email delivery outside this transaction. An outbox row written alongside token issuance lets a worker retry delivery without minting another token. Give the logical message a stable idempotency key, and make a resend either reuse the still-valid recovery grant or invalidate the old grant before issuing a new one; the policy matters less than ensuring that only the intended grant remains usable. Be careful with “reuse,” though: storing only a digest means the raw token cannot be reconstructed later. A design that resends the same link would need protected recoverable token material, which increases exposure. Invalidating and issuing afresh is usually the cleaner boundary.

Short path, hard rule: retries may repeat delivery, never authority.

## Resetting a password is also a session-revocation event

A stolen session survives a password change unless the application explicitly invalidates it. OWASP advises giving the user a choice to invalidate existing sessions or invalidating them automatically. For a recovery flow triggered because control may be lost, automatic revocation is the safer default: bump a server-side session version, delete active server sessions, or reject tokens issued before a per-account revocation timestamp.

Refresh tokens need the same treatment. OAuth 2.0 Security Best Current Practice describes refresh-token rotation and sender-constrained refresh tokens as ways to detect replay for public clients. In a rotation design, every refresh exchanges the current token for a new one; replay of an older member identifies a compromised token family and causes that family to be revoked. A completed password recovery should revoke all relevant families, not merely the browser session that submitted the form.

This is where ordering bites. If the password update commits but session revocation is left to a best-effort job, the attacker keeps a window of access. Put the revocation marker in the same database transaction as token consumption and password replacement. Cleanup of individual session rows can run later because authorization checks already consult the marker. The email saying “your password changed” is notification, not synchronization.

## Bot resistance needs layers that do not become identity oracles

Start with limits that don't require knowing the account: request budgets by IP prefix, device signal, and time window. Add account-scoped limits privately after lookup, plus velocity alerts for a single network probing many student addresses. Challenges can be introduced based on pre-lookup abuse signals, but they should not appear only for registered identities. Recovery pages should also avoid third-party resources that might receive a token-bearing URL through referrer data; OWASP recommends a referrer policy such as `noreferrer` on the reset page.

There is a real availability trade-off. Aggressive network throttles can block an entire dormitory, library, or school NAT, while weak limits allow cheap spraying. There isn't a universal threshold. Start with observed legitimate bursts, test shared-network behavior, and give support a documented escalation path that does not bypass identity proofing. Your mileage may vary during enrollment week.

Email-only recovery is not suitable when the mailbox is likely compromised, when high-value administrative roles require stronger assurance, or when students may lose access to an institution-managed address before they can recover. In those cases, use pre-enrolled recovery codes, a previously verified authenticator, or a staffed proofing process designed against social engineering. Conversely, avoid knowledge-based questions; NIST SP 800-63B says knowledge-based authentication or security questions shall not be used for authentication.

The catch is that stronger proofing raises support cost and lockout risk. A small learning platform with low-impact accounts may reasonably choose an email token plus complete session revocation. A university identity provider protecting grades, payments, and staff privileges should separate recovery tiers and require stronger evidence for privileged roles. The mechanism follows the harm model.

## Test the side channels and the unhappy transitions

Unit tests are necessary but too polite. Exercise duplicate worker delivery, concurrent token consumption, expiry at the boundary, a resend racing with consumption, and refresh-token replay after recovery. In deployment, run the schema change before code that depends on the new token state, and preserve backward compatibility until old workers have drained. Rollback must not restore acceptance of already consumed grants.

For enumeration, compare distributions rather than single timings. Send controlled batches for known and unknown test identities through the same edge, then inspect status, body length, headers, challenge behavior, and latency percentiles. Keep the identities synthetic and authorized. Alert on recovery volume, unique identifiers per source, delivery age, token-consumption failures, session revocations, and support-assisted recoveries, but keep raw tokens and passwords out of telemetry.

I use one release question as the final gate: can any retry, race, or observable branch turn “this identifier might exist” into information or turn one recovery grant into two successful authority changes? If the answer is unclear, the flow isn't ready. The operational goal is modest and testable: outsiders see one dull response, while the database enforces one irreversible security transition.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://www.rfc-editor.org/rfc/rfc9110.html
- https://www.rfc-editor.org/rfc/rfc9700.html
- https://pages.nist.gov/800-63-4/sp800-63b.html
