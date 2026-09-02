# Node.js Email Rate Limits for Background Work: An Operations Guide

**Short answer: for rate-limited email sending, place a durable queue ahead of the sender, enforce one shared rate budget per destination, and treat each background job as at-least-once work with an idempotency key.** The deciding constraint is recovery: a worker can stop after the remote service accepts a request but before its own database records that fact. A timer alone cannot resolve that ambiguity. The runbook must say which record is authoritative, when a lease expires, who owns the rate budget, and how an operator distinguishes a retry from a new request. Without those answers, a queue can look empty while delivery work is still uncertain. The code path should therefore make a narrow promise: persist intent, claim it for a limited lease, pace one send, record the observed result, and reconcile later delivery events against the same business action. Each step has an owner and a state transition, which makes a stopped worker a recoverable condition instead of a reason to guess whether to resend.

This is a runbook for Node.js teams that need rate-limited email sending from background jobs. It also answers the "cheapest SaaS backend" part of the comparison in the only useful operational sense: compare the cost of ownership, recovery, and visibility after you have established the required delivery behavior. A low monthly bill does not repair a duplicate password-reset message or explain an old job that disappeared from the dashboard.

Start with the ledger. Keep a durable record of the intent to send before a worker can call the mail service, and make that record the source of truth for retries and reconciliation.

## How should Node.js background jobs apply email sending rate limits?

Use two separate controls. Admission limits how much work the application accepts into its queue; dispatch pacing limits how quickly workers send to one account, domain, or other independently constrained destination. They solve different failures. Admission prevents an unbounded backlog from consuming storage and operator attention. Pacing prevents a horizontal worker fleet from turning one allowed rate into many simultaneous bursts.

The rate budget must have one owner. A process-local limiter is valid only when one process is the sole dispatcher. Once multiple replicas can send, store the limiter state in shared infrastructure or assign each budget to exactly one dispatcher. The worker acquires a token immediately before the remote call, not while it is waiting to claim a job. This keeps scarce send capacity available for work that is ready to run.

Three signals make the distinction visible:

- Oldest pending-job age measures whether available capacity can clear the queue.
- Limiter wait time shows that the downstream limit, rather than worker CPU, is binding.
- The gap between accepted requests and terminal delivery outcomes reveals unfinished reconciliation.

Don't increase concurrency as a reflex. It does not increase legitimate throughput after all workers are waiting on the same budget, and it can make retry bursts harder to inspect.

## Build the queue around an idempotent business action

The safest enqueue path is a transactional outbox when the application already has a relational database. The business write and the intention to deliver mail commit together. A later dispatcher reads the outbox; it does not infer an email from an application event that may have been lost between transactions.

Each row needs a stable job ID, a business-action idempotency key, a creation timestamp, `run_after`, an attempt count, a state, and enough correlation data to join provider events back to the record. The idempotency key identifies the action, not an attempt. Retrying one invitation reuses the same key; creating a later invitation creates another key. Avoid storing credentials or unnecessarily rendered private content in a queue table.

PostgreSQL documents `SKIP LOCKED` as an inconsistent view for general-purpose reads, while calling out its usefulness for multiple consumers accessing a queue-like table. That is a precise boundary: use it to claim work, never to produce a report or a customer-visible count. Claim quickly, set a lease, commit, then make the network call. Holding a database lock while waiting on a remote service converts network latency into database contention.

```go
package queue

import (
	"context"
	"database/sql"
)

type Job struct {
	ID             int64
	IdempotencyKey string
}

func Claim(ctx context.Context, db *sql.DB, worker string) (Job, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, err
	}
	defer tx.Rollback()

	var job Job
	err = tx.QueryRowContext(ctx, `
		SELECT id, idempotency_key
		FROM email_outbox
		WHERE state = 'pending' AND run_after <= now()
		ORDER BY run_after, id
		LIMIT 1
		FOR UPDATE SKIP LOCKED`).Scan(&job.ID, &job.IdempotencyKey)
	if err != nil {
		return Job{}, err
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE email_outbox
		SET state = 'running', lease_owner = $2,
		    lease_until = now() + interval '2 minutes'
		WHERE id = $1`, job.ID, worker)
	if err != nil {
		return Job{}, err
	}
	return job, tx.Commit()
}
```

The lease covers a process exit after claim. The idempotency key covers a replay after the remote call. Neither creates literal exactly-once delivery across a database and an external network. The practical target is an idempotent business outcome, a recorded request correlation ID, and a consumer that can safely process a duplicate delivery event. Keep the state machine explicit: `pending`, `running`, `accepted`, and documented terminal outcomes. `accepted` is an observation, not proof that the recipient received a message.

## Which backend shape fits the recovery requirement?

The comparison should start with recovery requirements, not a product scorecard. A cron job is a time-based job scheduler; it can wake a dispatcher or run a repair sweep, but it is not a durable job ledger. If one scheduled invocation overlaps another, the ledger and lease rules still decide whether a job runs once, waits, or is reclaimed.

| Backend shape | Fits well when | Operational trade-off | Not suitable when |
| --- | --- | --- | --- |
| Relational outbox | The application needs an atomic business write and enqueue | Polling, table maintenance, and retry operations remain with the team | The primary database cannot absorb queue traffic |
| Durable queue service | Workers need high dispatch volume and independent scaling | The system gains another stateful dependency and a consistency boundary | The team cannot operate or audit its persistence model |
| Hosted task queue | A small team needs managed scheduling and retry visibility | Enqueue may be separate from the application transaction | Policy requires local control of queued data |
| Workflow engine | Work has long waits, branches, compensation, or human steps | More concepts and storage must be operated | A send is a short, simple retryable action |
| Cron plus database scan | Periodic batches and repair sweeps are sufficient | Overlap control and per-job observability are weaker | A trigger is being used as the record of delivery intent |

For a Node.js codebase, matching the worker runtime is helpful but secondary. Test the candidate against shutdown points: stop after claim, after the external send, and before acknowledgment. Then inspect the resulting row and the replay behavior. A system with a pleasant SDK but no answer to those three cases has not solved the hard part.

The catch is operational ownership. Choose the relational pattern only when database operators accept its traffic and maintenance. Keep a managed queue when the team needs its persistence, retry tooling, and failover responsibilities carried outside the application. Use a workflow engine when compensation and multi-day state exist today, not as a placeholder for imagined complexity. Requirements around retention, data residency, and audit access can change that choice; validate them with the relevant policy owners.

## Verify the failure paths before rollout

Run a staged failure matrix before enabling real sends. Terminate a worker after it claims a row and verify lease expiry returns the job to eligible work. Interrupt it after a request has been made and verify the next attempt preserves the idempotency key. Run two workers and confirm they claim distinct rows. Send a burst above the configured rate budget and confirm the queue grows predictably without dropping intent. Feed duplicate delivery events and confirm the event consumer's unique constraint makes the second event harmless.

This is where many designs become vague. The ordinary happy path is short; the recordkeeping after an uncertain boundary is where the design earns its keep.

Alert on user risk: oldest pending age, unreclaimed expired leases, sustained permanent failures, and a growing interval between accepted and terminal states. Worker CPU and a successful cron invocation are supporting signals, not delivery evidence. Preserve correlation IDs across enqueue, dispatch, and delivery events; redact recipient information while retaining enough identity to trace the business action.

Deploy the consumer with sends disabled against synthetic jobs first, then compare state transitions and rate metrics with the established path. Assign one explicit owner to each rate budget during rollout and increase concurrency gradually. The rollback control should stop new claims while allowing bounded in-flight work to reach a recorded state. After leases or request timeouts pass, restore the prior consumer and reconcile each nonterminal row by idempotency key before replaying it.

Document two manual commands in the real runbook: replay one job and drain a bounded time range. Both should expose a dry-run count and create an audit record. "Run the cron again" is not a recovery procedure because a timer does not know which external side effect was already attempted.

## References

- Cron: https://en.wikipedia.org/wiki/Cron
- PostgreSQL `SELECT` documentation, including `FOR UPDATE SKIP LOCKED`: https://www.postgresql.org/docs/current/sql-select.html
