## Contract conventions

A few rules should apply to every component contract:

* IDs are opaque stable identities: `RequestId`, `ExecutionId`, `AttemptId`, `Key`, `PolicyScopeId`.
* Components exchange **domain values and decisions**, not each other’s internal mutable structures.
* A command either succeeds atomically or returns an explicit error. No partially-applied state mutation.
* State-owning components expose operations, not unrestricted mutable access.
* Cross-component references use IDs. An Execution Manager should not hold a mutable pointer into Key Registry state, for example.
* Events describe facts **after** their corresponding state transition has committed.

The most important transactional invariant is:

> **Any decision based on mutable shared state must be applied atomically with the state change that makes the decision valid.**

So `can_admit()` followed later by `record_admission()` is dangerous. Prefer an operation that actually reserves admission.

---

# Broker

The Broker is the application-level command handler.

### Inputs

`SubmitRequest(request)`

Where a Request contains resolved CLI configuration sufficient to derive:

* Command
* Key
* optional Group
* Execution Definition

### Dependencies

The Broker may call:

* Request Registry
* Key Registry
* Policy Scope Registry
* Contention Policy
* Admission Controller
* Pending Scheduler
* Execution Manager
* Event Sink

It must not call the OS Process Runner directly.

### Contract

`submit(request) -> RequestId | ApplicationError`

The Broker guarantees that before returning success:

* the Request has an authoritative Request Registry entry
* its Key's latest Execution Definition has been updated
* its initial disposition has been committed

Initial disposition must be one of:

* attached
* pending
* assigned to a fresh Execution
* dropped
* superseded/rejected as applicable

The Broker must never leave a successfully accepted Request in an undefined state.

---

# Request Registry

Authoritative owner of Request lifecycle.

### Core operations

`register(request) -> RequestId`

Creates a Request in `Received`.

`attach(request_id, execution_id)`

Associates the Request with an existing Execution.

`pend(request_id, key, scope)`

Marks the Request as waiting for fresh execution.

`subscribe(request_id, subscriber_id)`

Registers a transport subscription for Request and Execution events.

`unsubscribe(subscriber_id)`

Removes only the subscription. It never cancels the Request or Execution.

`assign(request_id, execution_id)`

Associates the Request with newly-created work.

`supersede(request_id, by_request_id)`

Terminates an obsolete Request.

`drop(request_id, reason)`

Terminates a Request without execution.

`complete(request_id, outcome)`

Records the final client-visible Outcome.

`requests_for_execution(execution_id) -> [RequestId]`

Provides routing information.

### Invariants

A Request has at most one active disposition.

A terminal Request cannot return to a non-terminal state.

A Request may reference at most one Execution at a time.

Attaching and assigning are semantically distinct even though both eventually associate a Request with an Execution.

---

# Key Registry

Authoritative owner of current desired state for each Key.

### Core operations

`upsert_definition(key, definition) -> DefinitionVersion`

Atomically replaces the current Execution Definition using last-wins semantics.

Returning a monotonically increasing `DefinitionVersion` would be useful.

`current_definition(key) -> ExecutionDefinition`

Returns the latest desired definition.

`active_execution(key) -> Option<ExecutionId>`

Returns currently active work for equivalence/reuse decisions.

`bind_scope(key, scope) -> ScopeBindingResult`

Establishes the Key's Policy Scope on first use. A different Scope is rejected while the
Key has active or pending work. Rebinding an idle Key is an explicit operation.

`set_active(key, execution_id)`

Claims the Key for an Execution.

`clear_active(key, execution_id)`

Clears the active reference only if it still points to that Execution.

That compare-by-ID behavior matters enormously for avoiding stale completion races.

### Invariants

For a given Key:

* exactly one current Execution Definition exists after first use
* at most one nonterminal Execution exists
* exactly one Policy Scope is bound after first use
* an active Execution always refers to an immutable snapshot, never the mutable current definition
* stale Execution completion cannot clear a newer active Execution

---

# Policy Scope Registry

Authoritative owner of shared pending-work and admission state.

### Core operations

`resolve_scope(key, group?) -> PolicyScopeId`

Pure identity resolution may happen elsewhere, but conceptually this is the mapping:

`group ?? key`

`configure(scope, policy)`

Updates Group policy configuration. The new configuration applies immediately to
existing pending Requests and future Admission decisions. Reconfiguration immediately
reapplies Contention Policy to the pending-work set; for example, changing to `Latest`
supersedes obsolete pending Requests for each Key. It does not revoke permits or mutate
existing Execution snapshots.

### Pending work

The Policy Scope owns the authoritative pending-work set for every Key in the scope.
It also owns the current Contention Policy used to change that set.

`apply_contention(scope, key, request_id) -> PendingDisposition`

Atomically evaluates same-Key active and pending work using the Scope's current policy.
It may attach the Request, retain it as pending, supersede same-Key pending Requests,
drop it, or request replacement of same-Key active work.

`select_pending(scope) -> Option<RequestId>`

Selects the **oldest runnable request**: the earliest received still-pending Request in
the Scope whose Key has no nonterminal Execution. Key-aware indexes are an
implementation detail of this scope-owned operation.

`remove_pending(scope, request_id)`

Removes a Request that became terminal or was assigned to an Execution.

### Admission state

The critical operation should be atomic:

`try_reserve(scope, now) -> AdmissionResult`

Possible results:

`Reserved(AdmissionPermit)`

`BlockedOnCapacity`

`BlockedUntil(timestamp)`

An `AdmissionPermit` represents already-consumed capacity.

This is much safer than:

`can_start() -> true`

followed by separately incrementing a counter.

### Completion

`release(permit)`

Releases concurrency capacity associated with an admitted Execution.

Rate-limit consumption usually remains historical rather than being released.

### Invariants

A permit can be released at most once.

Every active Execution admitted through a constrained Policy Scope owns exactly one live permit.

Concurrency count derives from live permits, not separately-maintained arithmetic if avoidable.

Rate-limit accounting happens at successful admission time.

---

# Admission Controller

Pure or near-pure domain service evaluating admission policy.

There are two reasonable boundaries here.

I’d make the **Policy Scope Registry responsible for atomic reservation**, while the Admission Controller owns the calculations it uses.

### Contract

`evaluate(policy, scope_state, now) -> AdmissionDecision`

Returns:

* `Admit`
* `BlockOnCapacity`
* `BlockUntil(timestamp)`

It does not mutate state.

The Policy Scope Registry can execute this decision while holding whatever synchronization protects scope state, then atomically issue the permit.

This keeps:

**policy mathematics in the domain**

while:

**atomicity remains with the state owner.**

---

# Contention Policy

Pure domain strategy.

### Contract

`decide(context) -> ContentionDecision`

Context contains immutable facts such as:

* configured mode
* whether same-Key Execution is active
* whether matching work is reusable
* whether pending work exists
* admission status if already known
* incoming Request identity

Possible decisions:

* `Attach(execution_id)`
* `Pend`
* `SupersedePendingAndPend`
* `Drop`
* `Replace(execution_id)`

Every fresh-work decision enters the Policy Scope's pending-work set before scheduler
selection. `Pend` need not encode application mechanics beyond that semantic intent.

### Guarantee

The decision contains no side effects.

Given identical domain inputs, it should return the same decision.

---

# Pending Scheduler

Owns **wakeup state**, not Request or queue truth.

### Inputs

`notify_pending(scope)`

Indicates that this scope contains work worth reconsidering.

`notify_capacity_changed(scope)`

Triggers reconsideration after an Execution releases capacity.

`schedule_reconsideration(scope, timestamp)`

Registers a future wakeup for rate-limited work.

### Core callback

When awakened:

`reconsider(scope)`

The Scheduler should ask authoritative components:

* which pending Request does the Policy Scope currently select?
* can it now be admitted?
* what is that Request's Key's latest definition?

It must not trust stale cached copies of Request/Key state.

### Output

Once work becomes runnable, Scheduler issues an application-level command equivalent to:

`StartPending(request_id)`

That can go through the Broker or a narrower execution orchestration service.

### Invariants

Scheduler indexes may be stale without corrupting correctness.

A stale wakeup should simply discover that no relevant pending work exists.

This is a very useful architectural property: **scheduler state should affect efficiency, not truth.**

---

# Execution Manager

Authoritative owner of Execution lifecycle.

### Creation

`start_execution(spec, permit) -> ExecutionId`

`spec` contains an immutable snapshot:

* Key
* Policy Scope
* Definition Version
* Command
* retry policy
* timeout policy
* other Execution-local configuration

It also receives the Admission Permit proving capacity has already been reserved.

### Request association

I’d keep Request ownership in Request Registry, but the Execution Manager may maintain subscriber/request IDs as an index if needed.

`add_request(execution_id, request_id)`

### Attempt callbacks

`attempt_started(execution_id, attempt_id)`

`attempt_completed(execution_id, attempt_id, attempt_outcome)`

The Execution Manager validates that the callback belongs to the Execution's currently expected Attempt.

A stale or duplicate callback must not advance state twice.

### Cancellation

`cancel(execution_id, reason)`

Requests termination of the active Attempt through the Process Runner.

Cancellation intent becomes Execution state immediately; process death may happen later.

### Completion

When retry policy says no further Attempts:

`ExecutionManager` commits final Execution Outcome, then coordinates:

* Request completion
* Key active-reference clearing
* Policy Scope permit release
* pending-work wakeup
* final event publication

This cleanup should be **idempotent**.

### Invariants

An Execution:

* has exactly one immutable Definition snapshot
* has at most one active Attempt
* completes exactly once
* releases its Admission Permit exactly once
* never retries after terminal completion

---

# Retry Policy

Pure domain service.

### Contract

`decide(config, attempt_history, last_outcome) -> RetryDecision`

Returns:

`Complete(final_outcome)`

or:

`RetryAfter(duration)`

Potentially later:

`RetryAt(timestamp)`

but duration is enough if scheduling occurs immediately.

### Guarantees

Retry Policy:

* never starts processes
* never sleeps
* never increments application state itself
* treats `--retries N` as N **additional** Attempts
* incorporates retry predicates, delay, backoff, and jitter into one decision

For deterministic testing, jitter should receive an injected random source or explicit random value rather than using global randomness internally.

---

# Process Runner

Owns one Attempt's OS-process lifecycle.

### Creation

`spawn(attempt_spec, event_sink) -> AttemptHandle`

Attempt specification contains only process concerns:

* Command
* timeout
* kill grace
* environment/process options

It should not contain:

* Key
* Group policies
* retry policy
* contention policy

unless IDs are included purely for correlation.

### Handle operations

`terminate(handle, reason)`

Initiates graceful termination.

`force_kill(handle)`

Normally internal after kill grace expires, but the abstraction may expose it.

### Events

Exactly one ordered lifecycle should be reported:

* process started, if spawn succeeds
* stdout/stderr chunks as available
* one terminal Attempt Outcome

A spawn failure is itself a terminal Attempt Outcome.

### Terminal guarantee

Every successfully accepted spawn operation eventually produces **exactly one terminal notification**, barring catastrophic process/runtime failure outside foxrun's recoverability guarantees.

That exactly-once terminal contract massively simplifies Execution Manager logic.

---

# Clock

Pure source of time.

### Contract

`now() -> Instant`

Use monotonic time for durations, retries, rate limits, timeouts, and kill grace wherever wall-clock semantics are unnecessary.

System date changes should not cause rate limits or retries to behave strangely.

---

# Timer Service

Owns future wakeups.

### Contract

`schedule(deadline, token)`

`cancel(token)`

When the deadline passes:

`TimerExpired(token)`

Tokens identify semantic wakeups such as:

* retry Execution X
* reconsider Policy Scope Y
* force-kill Attempt Z

### Guarantees

Consumers must tolerate duplicate or stale timer events.

This lets Timer cancellation be best-effort internally while correctness remains protected by state validation.

Again: **timers wake state machines; timers do not define truth.**

---

# Event Sink

Receives committed semantic facts.

### Contract

`publish(event)`

Events include enough IDs for correlation:

* RequestId
* Key
* ExecutionId
* AttemptId
* PolicyScopeId where relevant

### Ordering rule

Within one entity's lifecycle, events must reflect committed transition order.

For example:

`attempt_started`

must never be emitted before the Execution Manager considers that Attempt active.

### Delivery semantics

I would **not** require durable exactly-once event delivery initially.

Treat domain events primarily as observability/client-stream facts, while registries remain authoritative.

---

# Execution event streams and protocol v2

The v2 IPC protocol is a breaking replacement for the current acquire/attach protocol.
It exposes stable Request, Execution, and Attempt IDs; clients submit Requests and
subscribe to Execution event streams rather than owning process leases.

Each Execution has one bounded, ordered event stream. Lifecycle events and raw output
share a monotonically increasing Execution sequence number. Output also includes its
`AttemptId`, so retries remain visible as separate process lifecycles within one
Execution.

Subscriptions may request replay from a sequence cursor. Foxrun delivers retained replay
before live events in sequence order. If the cursor precedes retained history, foxrun
reports the truncation rather than silently claiming a complete replay.

---

# Definition snapshots

I’d make `DefinitionVersion` first-class even if it initially feels unnecessary.

Example:

`Key("build")`
→ definition v12
→ Execution E1 snapshots v12

Request arrives:

→ definition v13

E1 still reports:

`definition_version = 12`

Later Execution E2:

`definition_version = 13`

This gives you an extremely clean answer to every race involving last-wins configuration.

---

# Application transactions

There are a few operations that need especially strong boundaries.

### Fresh Execution start

Conceptually this must behave atomically as:

1. select a still-valid pending Request from the Policy Scope
2. reserve Policy Scope capacity
3. snapshot latest Execution Definition
4. create Execution
5. mark Key active
6. assign Request to Execution

If something fails midway, compensate so you don't leak:

* admission permits
* phantom active Executions
* Requests stuck in limbo

You don't necessarily need a database-style transaction, but you need an application-level transactional operation with explicit rollback.

### Execution completion

Likewise:

1. mark Execution terminal exactly once
2. clear Key active reference conditionally
3. release admission permit exactly once
4. complete associated Requests
5. schedule pending reconsideration
6. emit terminal events

This operation must be idempotent against duplicate callbacks.

### Latest supersession

Must atomically:

1. install new pending Request
2. identify previous pending Request
3. mark previous one superseded

There should never temporarily be two authoritative `latest` Requests.

---

# Dependency contracts

At the component level, I’d enforce roughly this graph:

**Broker**
→ Request Registry
→ Key Registry
→ Contention Policy
→ Policy Scope Registry / Admission
→ Execution orchestration
→ Pending Scheduler

**Pending Scheduler**
→ Request/Key read interfaces
→ Policy Scope admission
→ execution orchestration
→ Timer Service

**Execution Manager**
→ Retry Policy
→ Process Runner
→ Timer Service
→ Request Registry
→ Key Registry
→ Policy Scope Registry
→ Event Sink

**Policies**
→ domain values only

**Process Runner**
→ OS

**IPC**
→ Broker / Request event subscription

No dependency should point from:

* Domain → Broker
* Domain → Registry implementations
* Process Runner → Execution Manager internals
* Key Registry → Process Runner
* Policy Scope Registry → Key Registry
* Retry Policy → Timer Service
* Contention Policy → Scheduler

That gives you a very strong acyclic structure.

## The contracts reduce to four kinds of components

This is the abstraction I’d keep in mind while implementing:

**State owners**

* Request Registry
* Key Registry
* Policy Scope Registry
* Execution Manager

**Decision makers**

* Contention Policy
* Admission Policy
* Retry Policy

**Orchestrators**

* Broker
* Pending Scheduler
* Execution Manager at the Execution boundary

**Side-effect adapters**

* IPC
* Process Runner
* Timer Service
* Event Sink

And the rule connecting all four:

> **Orchestrators read state from its owner, ask policies for decisions, commit changes through the owner, then invoke side effects through adapters.**
