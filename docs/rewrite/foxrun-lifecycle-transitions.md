# Foxrun Lifecycle State Transitions

## Purpose

This document defines the authoritative lifecycle state machines for foxrun domain entities.

The goal is to make legal transitions explicit, prevent ambiguous intermediate states, and ensure asynchronous events such as process exit, timer expiry, cancellation, and new Requests cannot corrupt state.

The core principle is:

> **State transitions are authoritative. Events report transitions after they commit.**

---

# Request Lifecycle

A Request represents one client's intent.

## States

### Received

The Request has been accepted and registered, but its disposition has not yet been decided.

This should be transient.

### Attached

The Request is being satisfied by an existing Execution.

It does not require fresh execution.

### Pending

The Request requires fresh execution but has not yet been admitted.

### Assigned

The Request has been assigned to a newly-created Execution.

`Attached` and `Assigned` are intentionally distinct because they describe different contention outcomes, even though both ultimately wait on an Execution.

### Succeeded

Terminal.

The Execution satisfying the Request succeeded.

### Failed

Terminal.

The Execution satisfying the Request completed unsuccessfully.

### TimedOut

Terminal.

The Execution's final Outcome is timeout.

### Cancelled

Terminal.

The Request's work was cancelled.

### Dropped

Terminal.

Contention policy discarded the Request without execution.

### Superseded

Terminal.

A newer Request made this Request obsolete.

### Rejected

Terminal.

Foxrun accepted enough of the request to produce a semantic result but could not execute it due to policy or application-level rejection.

---

## Request transitions

```
Received
  ├──→ Attached
  ├──→ Pending
  ├──→ Assigned
  ├──→ Dropped
  ├──→ Superseded
  └──→ Rejected

Pending
  ├──→ Assigned
  ├──→ Superseded
  ├──→ Dropped
  ├──→ Cancelled
  └──→ Rejected

Attached
  ├──→ Succeeded
  ├──→ Failed
  ├──→ TimedOut
  └──→ Cancelled

Assigned
  ├──→ Succeeded
  ├──→ Failed
  ├──→ TimedOut
  └──→ Cancelled
```

All terminal states have no outgoing transitions.

---

## Request invariants

A Request:

* has exactly one current state
* may reference at most one Execution
* cannot move from one Execution to another after becoming Attached or Assigned
* completes exactly once
* cannot return from a terminal state
* cannot become Pending after attaching to an Execution

A Request being superseded does not imply that an already-running Execution must be cancelled unless the configured contention policy explicitly requires replacement.

---

# Execution Lifecycle

An Execution represents one logical performance of an Execution Definition.

Retries happen inside the same Execution.

## States

### Created

The Execution exists and owns an Admission Permit, but no Attempt has started yet.

This should normally be transient.

### Running

An Attempt is currently active.

### RetryWaiting

The previous Attempt failed or timed out and Retry Policy has scheduled another Attempt for the future.

No child process is active.

The Execution still owns its admission capacity.

### Cancelling

Cancellation has been requested and the active Attempt is being terminated.

### Succeeded

Terminal.

An Attempt completed successfully.

### Failed

Terminal.

The final Attempt failed and Retry Policy chose not to continue.

### TimedOut

Terminal.

The final relevant Attempt timed out and no retry remains.

### Cancelled

Terminal.

Execution cancellation completed.

---

## Execution transitions

```
Created
  ├──→ Running
  └──→ Cancelled

Running
  ├──→ Succeeded
  ├──→ Failed
  ├──→ TimedOut
  ├──→ RetryWaiting
  └──→ Cancelling

RetryWaiting
  ├──→ Running
  └──→ Cancelled

Cancelling
  └──→ Cancelled
```

All terminal states have no outgoing transitions.

---

## Execution invariants

An Execution:

* has exactly one immutable Execution Definition snapshot
* belongs to exactly one Key
* belongs to exactly one Policy Scope
* owns exactly one Admission Permit until terminal completion
* has at most one active Attempt
* creates Attempts sequentially
* never creates a new Attempt after becoming terminal
* completes exactly once
* releases its Admission Permit exactly once

Retry waiting does **not** release concurrency capacity.

An Execution remains logically active while waiting to retry.

This preserves the meaning:

> one admitted Execution may contain multiple Attempts

rather than allowing retries to compete for admission as unrelated work.

---

# Attempt Lifecycle

An Attempt represents one concrete child-process lifecycle.

## States

### Starting

Process creation has been requested but the OS process has not yet successfully started.

### Running

The child process exists and is active.

### Terminating

Graceful termination has been requested.

The process may still exit normally during the kill-grace period.

### Killing

Forced termination has been requested.

### Succeeded

Terminal.

The process exited successfully.

### Failed

Terminal.

The process exited unsuccessfully.

### TimedOut

Terminal.

The Attempt exceeded its configured timeout.

### Cancelled

Terminal.

The Attempt ended because its Execution was cancelled.

### SpawnFailed

Terminal.

The child process could not be started.

---

## Attempt transitions

```
Starting
  ├──→ Running
  ├──→ SpawnFailed
  └──→ Cancelled

Running
  ├──→ Succeeded
  ├──→ Failed
  ├──→ Terminating
  └──→ Cancelled

Terminating
  ├──→ Succeeded
  ├──→ Failed
  ├──→ TimedOut
  ├──→ Cancelled
  └──→ Killing

Killing
  ├──→ TimedOut
  └──→ Cancelled
```

All terminal states have no outgoing transitions.

---

# Timeout transition semantics

Attempt Timeout begins when the child process successfully enters `Running`.

When the timeout expires:

```
Running
  → Terminating
```

Foxrun requests graceful termination and records the termination cause as timeout.

During Kill Grace:

* if the process exits, the final Attempt Outcome remains `TimedOut`
* the process's actual exit status may be retained as metadata

If Kill Grace expires:

```
Terminating
  → Killing
```

Foxrun performs forced termination.

Once process death is observed:

```
Killing
  → TimedOut
```

The semantic Outcome is based on **why foxrun terminated the process**, not whatever incidental signal/exit code resulted from termination.

---

# Explicit cancellation semantics

Explicit cancellation follows similar mechanics but produces a different semantic Outcome.

```
Running
  → Terminating
  → Cancelled
```

or, after grace expiry:

```
Running
  → Terminating
  → Killing
  → Cancelled
```

The Process Runner therefore needs to track a **termination cause** separately from mechanical process state.

Possible causes initially:

* timeout
* execution cancellation
* replacement

`replacement` may map to Execution `Cancelled`, while retaining its reason as metadata.

---

# Attempt → Execution transitions

Attempt Outcomes drive Execution state through Retry Policy.

## Attempt succeeds

```
Attempt → Succeeded
```

therefore:

```
Execution Running → Succeeded
```

No Retry Policy evaluation is required beyond recognizing success as terminal.

## Attempt fails

```
Attempt → Failed
```

Retry Policy is evaluated.

If retryable and retry budget remains:

```
Execution Running → RetryWaiting
```

Otherwise:

```
Execution Running → Failed
```

## Spawn fails

`SpawnFailed` is treated as an unsuccessful Attempt.

Retry Policy determines whether process creation should be attempted again.

Therefore:

```
SpawnFailed
  ├── retry → Execution RetryWaiting
  └── no retry → Execution Failed
```

## Attempt times out

Retry Policy is evaluated.

If timeout is retryable and budget remains:

```
Execution Running → RetryWaiting
```

Otherwise:

```
Execution Running → TimedOut
```

## Attempt cancelled

Cancellation caused by Execution cancellation is not retryable.

```
Execution Cancelling → Cancelled
```

---

# Retry Lifecycle

Retry is not a standalone entity, but its transitions are important.

After an unsuccessful Attempt:

1. Execution Manager records the terminal Attempt Outcome.
2. Retry Policy evaluates the Attempt history.
3. Retry Policy returns either:

   * Complete
   * RetryAfter(duration)
4. If retrying:

   * Execution enters `RetryWaiting`
   * retry timer is scheduled
5. On valid timer expiry:

   * Execution transitions to `Running`
   * a new Attempt is created

Conceptually:

```
Running
  → failed Attempt
  → RetryWaiting
  → retry timer
  → Running
```

A stale retry timer received after the Execution has been cancelled or completed performs no transition.

---

# Key Lifecycle

Keys do not need a complex state machine.

A Key record effectively has two independent pieces of state:

## Current Execution Definition

```
Undefined
  → Defined(v1)
  → Defined(v2)
  → Defined(v3)
  → ...
```

Definition versions only move forward.

Existing Executions retain their previous snapshots.

## Active Execution

```
Idle
  → Active(execution_id)
  → Idle
```

A stale completion may attempt:

```
clear_active(old_execution_id)
```

but must only succeed if the Key still references that same Execution.

This compare-by-ID rule protects against races involving replacement or delayed completion handling.

---

# Pending Work Lifecycle

Pending status belongs to Requests, but the scheduling semantics deserve their own transition model.

A pending Request may be:

### WaitingForCapacity

Blocked because the Policy Scope has reached its concurrency limit.

Wake condition:

> capacity changes

### WaitingUntil

Blocked by a time-based admission constraint such as rate limiting.

Wake condition:

> monotonic deadline reached

A Request may move between these scheduling conditions as policies are reevaluated.

For example:

```
WaitingForCapacity
  → capacity available
  → rate limit checked
  → WaitingUntil
```

These are scheduler annotations, not new Request lifecycle states.

The authoritative Request state remains `Pending`.

---

# Contention transitions

Contention Policy determines how incoming Requests affect Request and Execution state.

## Reuse

Given:

* Request R is `Received`
* Execution E for the same Key is active

Transition:

```
R: Received → Attached(E)
```

No Execution lifecycle transition occurs.

---

## FIFO

If fresh work is admissible:

```
Request: Received → Assigned
new Execution: Created → Running
```

If not:

```
Request: Received → Pending
```

Existing pending Requests remain unchanged.

---

## Latest

If fresh work is admissible and there is no reason to wait:

```
Request: Received → Assigned
new Execution: Created → Running
```

If blocked and no Request is currently pending:

```
Request: Received → Pending
```

If blocked and old Request P is pending:

```
P: Pending → Superseded
R: Received → Pending
```

At most one authoritative latest-pending Request exists for that Key.

---

## Drop

If fresh work is admissible:

```
Request: Received → Assigned
```

If not:

```
Request: Received → Dropped
```

No pending state is created.

---

## Replace

Given active Execution E and new Request R:

```
E: Running → Cancelling
R: Received → Pending
```

Once E reaches `Cancelled` and releases its Admission Permit, R is reconsidered normally.

If Admission then succeeds:

```
R: Pending → Assigned
new Execution → Running
```

This avoids requiring replacement to magically bypass concurrency accounting.

Replacement changes priority/obsolescence semantics; it does not violate admission policies.

---

# Admission lifecycle

Admission itself should not become a long-lived stateful entity.

It is an atomic operation over Policy Scope state.

## Successful admission

```
try_reserve(scope)
  → Reserved(permit)
```

The permit immediately counts against concurrency and rate-limit state.

The caller must then either:

* create the Execution using that permit
* release the permit during rollback

A successful reservation must never be silently abandoned.

## Capacity block

```
try_reserve(scope)
  → BlockedOnCapacity
```

No policy state changes.

The Request remains Pending.

## Time block

```
try_reserve(scope)
  → BlockedUntil(t)
```

No admission capacity is reserved.

Scheduler records a wakeup for `t`.

---

# Admission Permit Lifecycle

Admission Permit is a useful small lifecycle of its own:

```
Reserved
  → BoundToExecution
  → Released
```

or during failed creation:

```
Reserved
  → Released
```

A permit must never be:

* bound to multiple Executions
* released multiple times
* retained after terminal Execution completion

---

# Policy Scope Lifecycle

A Policy Scope's derived operational state is mostly:

```
Available
Saturated
TimeBlocked
Saturated + TimeBlocked
```

These should not necessarily be represented as explicit enum states.

They are projections of authoritative data:

* live Admission Permits
* configured concurrency limit
* rate-limit history
* current monotonic time

Avoid storing redundant `is_saturated` or `is_rate_limited` booleans.

Compute them from source state.

---

# Execution completion transaction

Execution completion is one of the most important lifecycle boundaries.

When an Execution becomes terminal:

1. commit the terminal Execution state
2. prevent any further Attempt transitions
3. conditionally clear the Key's active Execution reference
4. release the Admission Permit
5. transition associated Requests to their final states
6. publish committed terminal events
7. notify Pending Scheduler that Policy Scope capacity changed

This operation must be idempotent.

If process-exit handling, cancellation, and timeout race with each other, only the first valid terminal transition wins.

Subsequent completion signals become stale observations.

---

# Replacement transaction

Replacement has an intentional two-phase lifecycle.

## Phase 1: supersede active work

```
active Execution → Cancelling
incoming Request → Pending
```

The newest Execution Definition has already been committed.

## Phase 2: start replacement

After old Execution completion:

```
old Execution → Cancelled
permit released
pending Request reconsidered
admission reserved
Request → Assigned
new Execution → Running
```

This design ensures:

* no accidental over-capacity execution
* no mutation of the old Execution definition
* replacement uses the latest definition
* multiple replacement Requests can still be governed by pending policy

---

# Latest + changing definitions

Example:

1. Execution E1 runs definition v1.
2. Request R2 arrives with definition v2.
3. R2 becomes Pending.
4. Request R3 arrives with definition v3.
5. R2 becomes Superseded.
6. R3 becomes Pending.
7. E1 completes.
8. R3 is admitted.
9. Execution E2 snapshots **v3**.

The pending Request identifies intent to perform fresh work.

The Execution Definition remains the authoritative description of what the next Execution should actually run.

---

# Request/Execution completion mapping

The final Execution Outcome maps to all Attached/Assigned Requests still associated with it.

```
Execution Succeeded
  → Request Succeeded

Execution Failed
  → Request Failed

Execution TimedOut
  → Request TimedOut

Execution Cancelled
  → Request Cancelled
```

Requests already terminal due to supersession/drop/rejection must not be transitioned again.

---

# Event ordering

Events must describe committed state.

For a simple successful execution:

```
request_received
execution_started
attempt_started
attempt_completed
execution_completed
request_completed
```

For reuse:

```
request_received
request_attached
...
execution_completed
request_completed
```

For retry:

```
attempt_started
attempt_failed
retry_scheduled
attempt_started
attempt_completed
execution_completed
```

For latest supersession:

```
request_received(R2)
request_pending(R2)
request_received(R3)
request_superseded(R2)
request_pending(R3)
```

Events may contain additional diagnostic data, but their ordering must never imply a transition that has not yet committed.

---

# Race handling

Asynchronous inputs must be treated as conditional transition requests.

## Stale Attempt completion

If Attempt A1 reports completion after Execution E has already moved to another Attempt or terminal state:

> ignore it as stale

It must not mutate Execution state.

## Stale timer

If a retry/rate-limit/kill timer fires after its associated state no longer exists:

> ignore it

## Duplicate process termination event

Only the first valid terminal Attempt transition commits.

## Execution completion versus replacement

`clear_active(key, execution_id)` must be conditional.

An old Execution must never clear the active reference belonging to a newer Execution.

## Client disconnect

Client disconnection must not implicitly mutate Execution state unless explicit Request lifecycle policy says it should.

Transport failure and execution cancellation remain separate concepts.

---

# Illegal transitions

Illegal transitions should fail loudly in development/testing rather than silently coercing state.

Examples:

* terminal Request → Pending
* terminal Execution → RetryWaiting
* RetryWaiting → Succeeded without another Attempt
* Execution with active Attempt → start second Attempt
* Released Admission Permit → release again
* Superseded Request → Assigned
* Attempt Succeeded → TimedOut
* Execution Definition version moving backward

Production handling may convert impossible external races into ignored stale events where appropriate, but internal invariant violations should remain observable.

---

# Canonical state machines

The three core lifecycle machines can be summarized as:

## Request

```
Received
  ↓
Attached | Pending | Assigned
  ↓
Succeeded | Failed | TimedOut | Cancelled
```

with direct terminal alternatives:

```
Received/Pending
  → Dropped | Superseded | Rejected
```

## Execution

```
Created
  ↓
Running
  ↕
RetryWaiting
```

and optionally:

```
Running → Cancelling
```

eventually:

```
Succeeded | Failed | TimedOut | Cancelled
```

## Attempt

```
Starting
  ↓
Running
  ↓
Terminating
  ↓
Killing
```

with terminal outcomes:

```
Succeeded | Failed | TimedOut | Cancelled | SpawnFailed
```

---

# Core Lifecycle Invariants

1. **Requests express intent; Executions satisfy intent; Attempts perform OS work.**
2. **A Request becomes terminal exactly once.**
3. **An Execution becomes terminal exactly once.**
4. **An Attempt becomes terminal exactly once.**
5. **An Execution has at most one active Attempt.**
6. **Retries create new Attempts within the same Execution.**
7. **Retry waiting does not release Execution admission capacity.**
8. **Execution Definitions are versioned and immutable once snapshotted.**
9. **New Requests may update desired state without mutating active Executions.**
10. **Only successful Admission creates capacity ownership.**
11. **Every Admission Permit is released exactly once.**
12. **Pending Scheduler wakeups never constitute authoritative state.**
13. **Stale asynchronous events are harmless.**
14. **Terminal events are emitted only after terminal state commits.**
15. **Replacement cancels old work before admitting replacement work normally.**
16. **Latest supersedes pending intent, never mutates the active Execution.**
17. **Key state determines sameness; Policy Scope state determines capacity.**

---

# Mental Model

The lifecycle can be reduced to:

> **A Request either shares work, waits for work, starts work, or terminates without work.**

> **An Execution owns admission capacity and performs Attempts until it reaches one final Outcome.**

> **An Attempt owns exactly one OS process lifecycle.**

> **Everything asynchronous—timers, process exits, client disconnects—is merely a request to perform a state transition. The authoritative state machine decides whether that transition is still valid.**

