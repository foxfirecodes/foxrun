# Foxrun Domain Model

## Purpose

Foxrun is a command runner that applies execution policies such as deduplication, concurrency limits, queueing, rate limiting, timeouts, and retries.

The domain model separates **requests for work**, **logical executions**, and **individual process attempts** so these policies can compose without conflating user intent with process lifecycle.

## Ubiquitous Language

### Request

One client request asking foxrun to execute a Command under a set of policies.

A Request does not necessarily result in a new process. It may attach to an existing Execution, remain pending, supersede other work, or be dropped according to the Contention Policy.

### Command

The process-launch specification describing what should run.

Includes:

- executable
- arguments
- working directory
- other relevant process-launch configuration

### Key

The logical identity of requested work.

By default, the Key is derived from the canonical working directory and Command. An explicit `--key` overrides derived identity.

The Key answers:

> Are these Requests asking for the same logical work?

### Policy Scope

The state-sharing boundary for execution policies.

By default:

`Policy Scope = Key`

When `--group` is specified:

`Policy Scope = Group`

This allows otherwise-independent Keys to share policies such as concurrency and rate limits.

The Policy Scope answers:

> Which Requests and Executions compete for the same policy capacity?

### Group

An explicitly named Policy Scope shared by multiple Keys.

Groups do not define work equivalence. Two different Keys in the same Group remain different work and cannot reuse each other's Executions.

Groups exist solely to share policy state that would otherwise be scoped independently to each Key.

### Execution Definition

The latest desired Command and execution policies associated with a Key.

New Requests update the Execution Definition using **last-wins semantics**.

An already-running Execution is not mutated when the definition changes. Each Execution receives a snapshot of the Execution Definition when it starts.

This separates:

> What is running now?

from:

> What should run next?

### Execution

One logical performance of an Execution Definition.

An Execution:

- belongs to one Key
- executes within one Policy Scope
- uses a snapshot of an Execution Definition
- may satisfy multiple Requests
- contains one or more Attempts
- produces one final Outcome

Retries remain part of the same Execution.

### Attempt

One concrete child-process lifecycle within an Execution.

The first process start creates the first Attempt. Each retry creates another Attempt within the same Execution.

An Attempt owns process-level state such as:

- process handle / PID
- start and end times
- stdout and stderr
- exit status
- terminating signal
- timeout and termination state

The distinction is strict:

> Request ≠ Execution ≠ Attempt.

For example, three Requests may share one Execution which itself requires three Attempts before succeeding.

### Attachment

The relationship created when a Request is satisfied by an already-active Execution rather than creating fresh work.

Attachment is the mechanism underlying deduplication/reuse.

Attached Requests observe the shared Execution's output and receive its final Outcome.

### Pending Request

A Request that has been accepted but has not yet been assigned to a new Execution.

Pending Requests are governed by the Contention Policy and wait for Admission.

### Admission

The decision that fresh work may begin within a Policy Scope.

Admission policies initially include:

- maximum concurrency
- rate limiting

Admission answers:

> May another Execution start now?

### Contention Policy

The policy determining what happens when a Request encounters existing work or cannot immediately start a fresh Execution.

The supported behaviors are:

#### Reuse

Attach to an active Execution with the same Key.

No new Execution is created.

#### FIFO

Preserve waiting Requests in arrival order.

Each Request eventually produces fresh work as Admission permits.

#### Latest

Keep only the newest pending Request for the relevant identity.

A newer Request supersedes an older pending Request. Once admitted, one fresh Execution starts using the latest Execution Definition.

This provides trailing-edge coalescing without accumulating redundant work.

#### Drop

Discard a Request when fresh work cannot immediately begin.

The Request does not attach to existing work and does not remain pending.

#### Replace

Newer work supersedes active work.

The active Execution is cancelled and fresh work using the latest Execution Definition begins once Admission permits.

### Supersession

The replacement of obsolete work by newer intent.

`latest` supersedes **pending** work.

`replace` supersedes **active** work.

### Concurrency Limit

An Admission constraint limiting the number of active Executions within a Policy Scope.

For example:

`--max-concurrency 2`

permits at most two active Executions sharing that Policy Scope.

### Rate Limit

An Admission constraint limiting how frequently Executions may start within a Policy Scope.

For example:

`--rate-limit 10/1m`

allows at most ten Execution admissions per minute.

Rate limiting governs Execution starts, not the number of currently active Executions.

### Retry Policy

The policy determining whether and when an Execution should create another Attempt after an unsuccessful Attempt.

It consists of:

- Retry Limit
- Retry Predicate
- Retry Delay
- Backoff
- Jitter

Retries never create a new Execution.

### Retry Limit

The maximum number of additional Attempts permitted after the initial Attempt.

`--retries 3`

therefore permits up to four Attempts total.

### Retry Predicate

Determines whether a particular Attempt Outcome is retryable.

CLI configuration may explicitly include or exclude exit codes using `--retry-on` and `--no-retry-on`.

### Retry Delay

The base delay before beginning another Attempt.

### Backoff

The strategy controlling how Retry Delay changes after successive failures.

Initially:

- fixed
- exponential

### Jitter

Random variation applied to calculated Retry Delays.

Jitter prevents independent executions from repeatedly retrying simultaneously against a shared external dependency.

### Attempt Timeout

The maximum duration an individual Attempt may run.

Timeout applies independently to each retry.

For example:

`--timeout 10s --retries 3`

permits four Attempts of up to ten seconds each, plus any time spent waiting between retries.

There is no overall Execution deadline.

### Graceful Termination

An initial request for an active child process to terminate, normally using `SIGTERM`.

### Kill Grace

The amount of time foxrun allows for Graceful Termination before escalating to Forced Termination.

### Forced Termination

Termination of a child process after its Kill Grace expires, normally using `SIGKILL`.

### Outcome

The semantic result of a Request, Execution, or Attempt.

Outcomes should not be represented internally solely as process exit codes.

Relevant outcomes include:

- Succeeded
- Failed
- Timed Out
- Cancelled
- Dropped
- Superseded
- Rejected

Process-specific metadata such as exit code or terminating signal may accompany an Outcome.

Mapping Outcomes onto foxrun's own process exit status occurs at the CLI boundary.

---

## Identity and Policy Scoping

Key and Policy Scope answer different questions.

**Key defines work equivalence.**

**Policy Scope defines shared policy state.**

Without an explicit Group, each Key receives its own Policy Scope:

    Key A → Scope A
    Key B → Scope B
    Key C → Scope C

With a Group:

    Key A ─┐
    Key B ─┼→ Group "builds"
    Key C ─┘

The three Keys now share concurrency and rate-limit state while remaining distinct work.

An Execution for Key A can therefore consume capacity that prevents Key B from being admitted, but a Request for Key B can never reuse an Execution for Key A.

---

## Execution Definition Semantics

Each Key has a current Execution Definition.

When a Request arrives, its Command and policy configuration become the latest definition for that Key.

Updates use **last-wins semantics**.

For example:

1. Request A defines `build-v1`.
2. Execution A starts from that definition.
3. Request B arrives for the same Key but defines `build-v2`.
4. The Key's current Execution Definition becomes `build-v2`.
5. Execution A continues unchanged with its original snapshot.
6. The Contention Policy determines what happens to Request B.
7. If another Execution eventually starts, it uses the latest definition.

Policy updates and Command updates therefore follow the same semantics.

---

## Contention and Admission

Contention Policy and Admission are separate concerns.

**Contention Policy** determines what should happen to competing Requests.

**Admission** determines whether fresh work may start now.

A Request requiring fresh work is evaluated against the Policy Scope's:

- active Execution count
- concurrency limit
- rate-limit state

If Admission fails, the Contention Policy determines whether the Request remains pending, supersedes other work, is dropped, or causes active work to be replaced.

Reuse is special in that no fresh Execution is required: a matching Request may instead attach to existing work for the same Key.

The CLI may expose this concept as:

`--when-busy <reuse|fifo|latest|drop|replace>`

The domain model does not prescribe which behavior must ultimately be foxrun's default.

---

## Execution and Retry Lifecycle

An admitted Request creates an Execution from a snapshot of the latest Execution Definition.

The Execution creates its first Attempt.

An Attempt may:

- succeed
- fail
- time out
- be cancelled

After an unsuccessful Attempt, the Retry Policy determines whether another Attempt should occur.

If another Attempt is allowed:

1. evaluate the Retry Predicate
2. calculate Retry Delay
3. apply Backoff
4. apply Jitter
5. wait
6. create the next Attempt

When no further Attempt is permitted or required, the Execution reaches its final Outcome.

That Outcome is propagated to the Requests satisfied by the Execution.

---

## Domain Flow

The normal flow is:

**Request → Key → Execution Definition → Policy Scope → Contention → Admission → Execution → Attempt(s) → Outcome**

In detail:

1. Receive a Request.
2. Resolve its Key.
3. Update the Key's Execution Definition using last-wins semantics.
4. Resolve its Policy Scope: explicit Group or Key by default.
5. Apply the Contention Policy against existing work.
6. If fresh work is required, evaluate Admission.
7. Pending work waits, is superseded, or is dropped according to contention semantics.
8. Once admitted, create an Execution from the latest Execution Definition.
9. Create its first Attempt.
10. Enforce Attempt Timeout and termination behavior.
11. Evaluate Retry Policy after unsuccessful Attempts.
12. Create additional Attempts as required.
13. Produce the final Execution Outcome.
14. Propagate the Outcome to Requests satisfied by that Execution.
15. Re-evaluate pending work whenever relevant policy state changes.

---

## Domain Events

Structured output should expose domain events rather than arbitrary implementation logs.

Potential events include:

### Request events

- `request_received`
- `request_attached`
- `request_pending`
- `request_superseded`
- `request_dropped`

### Execution events

- `execution_started`
- `execution_cancelled`
- `execution_completed`

### Attempt events

- `attempt_started`
- `attempt_failed`
- `attempt_timed_out`
- `attempt_completed`
- `retry_scheduled`

### Admission events

- `admission_blocked`
- `admission_granted`

These preserve the Request / Execution / Attempt distinction for machine consumers.

---

## Core Invariants

The domain should maintain the following invariants:

1. **A Key defines work equivalence.**
2. **A Policy Scope defines shared policy state.**
3. **A Group may contain many Keys without making those Keys equivalent.**
4. **An Execution uses an immutable snapshot of an Execution Definition.**
5. **New Requests update the current Execution Definition using last-wins semantics.**
6. **An Execution contains one or more Attempts.**
7. **Retries create Attempts, never Executions.**
8. **Multiple Requests may be satisfied by one Execution through Attachment.**
9. **Only Executions consume concurrency capacity.**
10. **Only Execution admission consumes rate-limit capacity.**
11. **Attempt Timeout applies independently to each Attempt.**
12. **Contention Policy determines disposition of competing Requests; Admission determines whether fresh work may start.**

---

## Preferred Vocabulary

Use these terms consistently in code, documentation, and design discussion:

- Request
- Command
- Key
- Policy Scope
- Group
- Execution Definition
- Execution
- Attempt
- Attachment
- Pending Request
- Admission
- Contention Policy
- Supersession
- Retry Policy
- Attempt Timeout
- Outcome

Avoid using ambiguous terms as domain concepts:

- Job
- Task
- Run
- Invocation
- Session
- Worker

Reserve **Process** specifically for the actual operating-system child process owned by an Attempt.

---

## Mental Model

The shortest useful description of the domain is:

> **Key defines sameness. Policy Scope defines shared limits. Execution Definition defines what should run next. Contention Policy defines what happens while busy. Execution and Attempt define the work that actually runs.**

Or structurally:

**Requests express intent. Executions perform logical work. Attempts run processes. Policies decide when and how that work happens.**