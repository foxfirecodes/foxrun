# Foxrun Component Architecture

## Purpose

This document defines the component architecture for foxrun: component boundaries, responsibilities, state ownership, dependency direction, and interaction rules.

It is intentionally independent of the current implementation.

The architecture follows one central rule:

> **The domain decides what should happen. Infrastructure only makes it happen.**

Policies produce decisions. Orchestrators apply them. State owners mutate authoritative state. Adapters perform external side effects.

---

# Architectural Layers

Foxrun is divided into three conceptual layers:

    Adapters
        ↓
    Application
        ↓
    Domain

Dependencies point inward.

## Domain

Contains the execution model and policy decisions.

Examples:

- Request
- Command
- Key
- Policy Scope
- Execution Definition
- Execution
- Attempt
- Outcome
- Contention Policy
- Admission Policy
- Retry Policy

The Domain must not depend on application orchestration or infrastructure.

In particular, Domain code should not depend directly on:

- IPC
- serialization formats
- CLI parsing
- OS process APIs
- Unix sockets
- Tokio
- filesystem paths used for transport
- wall-clock sleeps

Domain behavior should be deterministic wherever practical.

## Application

Coordinates domain objects and state-owning components.

Examples:

- Broker
- Execution Manager
- Pending Scheduler
- Request Registry
- Key Registry
- Policy Scope Registry
- event routing

Application code decides **when to ask domain policies for decisions and how to apply those decisions**.

It does not contain OS-specific process behavior or transport-specific behavior.

## Adapters

Connect foxrun to the outside world.

Examples:

- CLI client
- IPC server
- Process Runner
- system clock/timer implementation
- structured event output

Adapters translate external behavior into application/domain concepts and execute requested side effects.

---

# Architectural Rules

The following rules are prescriptive.

## Policies produce decisions

Policies must describe what should happen without directly performing unrelated state mutations or external side effects.

For example:

    ContentionPolicy.decide(...) → SupersedePending(request_id)

The policy must not itself:

- kill a process
- mutate the broker
- write to IPC
- manipulate timers
- emit CLI output

The application layer applies the decision through the appropriate components.

## Orchestrators coordinate

Application orchestrators connect components and apply domain decisions.

They should not become authoritative owners of unrelated state.

The Broker, in particular, must remain an orchestrator rather than becoming a monolithic state machine.

## Registries own durable application state

Mutable state must have a clear authoritative owner.

Other components may hold identifiers or indexes referring to that state, but must not maintain competing authoritative copies.

## Adapters perform side effects

Operating-system and transport side effects belong behind explicit boundaries.

For example:

- Process Runner owns process spawning and signaling.
- IPC owns client transport.
- Timer infrastructure owns sleeping and wakeups.

Domain policy must not perform these operations directly.

---

# CLI Client

The CLI Client is a thin adapter between the user's shell and the foxrun broker.

## Responsibilities

The CLI Client:

- parses command-line arguments
- constructs a Request
- connects to the broker
- submits the Request
- receives output and lifecycle events
- renders human-readable or structured output
- receives the final Outcome
- maps the Outcome to the CLI process exit status

## Non-responsibilities

The CLI Client must not:

- enforce execution policies
- determine contention behavior
- maintain concurrency state
- perform retries
- spawn the requested Command directly when brokered execution is required
- infer broker state

The CLI should remain replaceable by another client without changing execution semantics.

---

# IPC Server

The IPC Server is the transport adapter through which clients communicate with the application.

## Responsibilities

The IPC Server:

- accepts client connections
- decodes protocol messages
- forwards Requests into the application layer
- delivers events, output, and Outcomes to connected clients
- detects connection lifecycle events

## Non-responsibilities

The IPC Server must not decide:

- whether work is equivalent
- whether work may start
- whether work should be reused
- whether a retry should occur
- whether pending work should be superseded

Transport semantics must remain separate from execution semantics.

---

# Broker

The Broker is the primary application service and entry point for Requests.

Its role is coordination.

## Responsibilities

When a Request arrives, the Broker coordinates:

1. Request registration
2. Key resolution
3. Execution Definition update
4. Policy Scope resolution
5. contention evaluation
6. admission evaluation where fresh work is required
7. creation, attachment, pending disposition, cancellation, or dropping of work
8. event publication

The Broker invokes the components responsible for each operation.

## Non-responsibilities

The Broker must not directly own:

- process handles
- retry timers
- concurrency counters
- rate-limit history
- Key definitions
- client transport
- OS process behavior

The Broker should not implement individual policy algorithms inline.

If the Broker becomes the place where every execution state transition is implemented, responsibilities must be extracted into their proper components.

---

# Request Registry

The Request Registry owns Request lifecycle and client relationships.

## State Ownership

For each Request, it owns:

- Request ID
- Request state
- subscription IDs, independent of Request lifetime
- associated pending work or Execution
- final Request Outcome

## Responsibilities

The Request Registry:

- registers incoming Requests
- records Request state transitions
- records Attachments
- associates Requests with Executions
- routes relevant events and output to interested clients
- records terminal Request Outcomes
- handles client disconnection according to defined lifecycle semantics

## Boundary

A client connection and a Request are distinct concepts.

An Execution must not inherently depend on the lifetime of the IPC connection that originally requested it.

---

# Key Registry

The Key Registry owns per-Key state.

The Key defines work equivalence.

## State Ownership

For each Key, the registry owns:

- bound Policy Scope ID
- current Execution Definition
- reference to active Execution, if any

The active Execution itself remains owned by the Execution Manager. The Key Registry stores only its identity/reference.

## Responsibilities

The Key Registry:

- resolves or registers Keys
- establishes or validates the Key's Policy Scope binding
- maintains the latest Execution Definition
- applies last-wins definition updates
- exposes active work for reuse or replacement

## Invariant

> **The Key Registry owns sameness, not capacity.**

It must not own Group concurrency counters or Group rate-limit state.

A Key has exactly one bound Policy Scope and at most one nonterminal Execution. A
submission with a different Scope is rejected while that Key has active or pending work;
an idle Key may be rebound only through an explicit operation.

---

# Policy Scope Registry

The Policy Scope Registry owns shared pending-work and admission state.

A Policy Scope is identified by:

- the Key when no Group is specified
- the Group when a Group is specified

## State Ownership

For each Policy Scope, the registry owns:

- current Contention Policy configuration
- authoritative pending Request membership and Key-aware pending indexes
- group-wide request-received ordering
- active Execution references/count
- concurrency configuration and state
- rate-limit configuration and state
- admission-related state required to determine future eligibility

## Responsibilities

The Policy Scope Registry:

- resolves Policy Scopes
- maintains the authoritative pending-work set and Key-aware indexes
- applies the Scope's current Contention Policy to pending work
- selects the oldest runnable pending Request for Admission
- tracks Executions entering and leaving a scope
- maintains concurrency state
- maintains rate-limit state
- exposes admission state to the Admission Controller

## Invariant

> **The Policy Scope Registry owns capacity, not sameness.**

It must not determine whether two Commands represent equivalent work.

Two Keys sharing a Group compete for capacity but remain distinct work.

---

# Contention Policy

Contention Policy is pure domain logic describing what should happen when Requests encounter existing or blocked work.

## Inputs

A contention decision may consider:

- incoming Request
- matching Key state
- active Execution state
- pending state
- whether fresh work can currently be admitted
- configured contention behavior

## Decisions

Contention Policy should return explicit decisions such as:

- Attach
- Start Fresh
- Pend
- Supersede Pending
- Drop
- Cancel and Replace

## Supported Behaviors

### Reuse

Attach the Request to an active Execution with the same Key.

### Queue

Keep the Request in the Policy Scope's pending-work set for eventual fresh execution.

### Latest

Keep only the newest pending Request for the same Key within the Policy Scope.

New Requests supersede obsolete pending Requests.

### Drop

Discard the incoming Request if fresh execution cannot immediately begin.

### Replace

Supersede active work by cancelling the current Execution and making the newest work eligible to execute.

## State

Contention Policy should contain little or no mutable runtime state.

Registries own state.

The policy interprets state and produces decisions.

---

# Admission Controller

The Admission Controller determines whether fresh work may start within a Policy Scope.

## Responsibilities

The Admission Controller evaluates all applicable admission constraints.

Initially:

- concurrency limits
- rate limits

It answers:

> **May a new Execution enter this Policy Scope now?**

## Results

Admission results should distinguish why work is blocked.

For example:

- Admitted
- Blocked on Capacity
- Blocked until Time

A boolean result is insufficient because different constraints require different wakeup mechanisms.

Concurrency becomes eligible when another Execution leaves the scope.

Rate-limited work becomes eligible when time advances.

## State

Admission policies should not independently duplicate Policy Scope state.

They evaluate state owned by the Policy Scope Registry.

---

# Pending Scheduler

The Pending Scheduler coordinates reconsideration of work that could not previously begin.

## Responsibilities

The Pending Scheduler:

- indexes pending work by Policy Scope
- reacts when concurrency capacity becomes available
- schedules wakeups for time-based admission constraints
- re-evaluates pending work through the Admission Controller
- requests creation of newly admitted Executions
- continues evaluation while additional capacity remains

## Boundary

The Policy Scope Registry owns which pending Requests exist in its scope and the
current Contention Policy that changes that set. The Pending Scheduler drives
reconsideration and selection using that authoritative Group state.

The Scheduler answers:

> **Which pending Request in this Policy Scope should be considered now?**

The Policy Scope's Contention Policy answers:

> **Which pending Requests for each Key should survive?**

## Queue Representation

Each Policy Scope owns one pending-work set spanning every Key in the scope. It may use
per-Key indexes internally for same-Key contention decisions, but those indexes do not
create Key-local queues. The Policy Scope is the authoritative queue owner.

The initial selection rule is **oldest runnable request**: choose the earliest received
pending Request whose Key has no nonterminal Execution. This is group-wide ordering with
work-conserving behavior; it is not strict FIFO head-of-line blocking.

---

# Execution Manager

The Execution Manager owns logical Execution lifecycle.

## State Ownership

For each Execution, it owns:

- Execution ID
- Key
- Policy Scope
- immutable Execution Definition snapshot
- associated Request IDs
- current Attempt
- Attempt history required for retry decisions
- retry state
- final Outcome

## Responsibilities

The Execution Manager:

- creates Executions from Execution Definition snapshots
- creates the initial Attempt
- reacts to Attempt completion
- evaluates Retry Policy
- schedules subsequent Attempts
- requests Execution cancellation
- determines final Execution Outcome
- publishes Execution lifecycle events
- releases completed Executions from active state

## Boundary

The Execution Manager owns logical execution but does not directly spawn or signal OS processes.

It delegates process lifecycle to the Process Runner.

---

# Process Runner

The Process Runner is the operating-system adapter for an Attempt.

Its responsibility is deliberately narrow:

> **Turn an Attempt specification into one child-process lifecycle.**

## Responsibilities

The Process Runner:

- spawns child processes
- manages process groups where required
- captures stdout and stderr
- reports process start
- observes process exit
- reports exit status or terminating signal
- enforces Attempt Timeout
- requests graceful termination
- enforces Kill Grace
- performs forced termination

## Outputs

The Process Runner reports process-level facts upward, such as:

- Started
- Stdout
- Stderr
- Exited
- Signaled
- Timed Out
- Cancelled

## Non-responsibilities

The Process Runner must know nothing about:

- retries
- Groups
- rate limits
- concurrency limits
- contention policies
- deduplication
- Attachments
- pending Requests

A timed-out Attempt is reported upward.

Whether that timeout causes a retry is an Execution-level decision.

---

# Retry Policy

Retry Policy is pure domain logic operating within an Execution.

## Inputs

Retry decisions consider:

- retry configuration
- current Attempt number
- Attempt Outcome

## Result

Retry Policy produces one of:

- Complete
- Retry After a calculated duration

## Responsibilities

Retry Policy composes:

- Retry Limit
- Retry Predicate
- Retry Delay
- Backoff
- Jitter

It calculates when another Attempt should become eligible.

## Boundary

Retry Policy does not sleep or create timers.

It may decide:

> Retry after 4.38 seconds.

Timer infrastructure is responsible for making the Execution runnable again at that time.

---

# Clock and Timer Service

Time must exist behind an explicit abstraction.

## Responsibilities

Clock/Timer infrastructure supports:

- rate-limit calculations
- rate-limit wakeups
- retry delays
- Attempt Timeout
- Kill Grace

Domain and application code should consume explicit time values or scheduling abstractions rather than directly scattering wall-clock calls and sleeps throughout the system.

## Testing Requirement

The abstraction must permit controlled or virtual time in tests.

Tests should be able to express behavior such as:

1. execution fails
2. retry scheduled for ten seconds
3. advance clock ten seconds
4. retry becomes eligible

without waiting ten real seconds.

---

# Event Sink

Domain and application lifecycle events should be published through an explicit event boundary.

## Responsibilities

The Event Sink accepts structured events such as:

- Request received
- Request attached
- Request pending
- Request superseded
- Execution started
- Attempt started
- retry scheduled
- Attempt timed out
- Execution completed

Consumers may translate these events into:

- human-readable CLI output
- JSON lifecycle events
- diagnostics
- future observability integrations

## Boundary

Core components emit semantic events.

They do not format terminal output or JSON directly.

---

# State Ownership

Every piece of mutable state must have one authoritative owner.

| State | Authoritative Owner |
|---|---|
| Request lifecycle | Request Registry |
| Client subscription ↔ Request/Execution relationship | Request Registry |
| Key ↔ Policy Scope binding | Key Registry |
| Current Execution Definition | Key Registry |
| Active Execution reference for a Key | Key Registry |
| Pending Request membership and contention policy | Policy Scope Registry |
| Work equivalence | Key domain |
| Shared concurrency state | Policy Scope Registry |
| Shared rate-limit state | Policy Scope Registry |
| Pending wakeup/index state | Pending Scheduler |
| Execution lifecycle | Execution Manager |
| Attempt/retry history | Execution Manager |
| OS process handle/PID/pipes | Process Runner |
| Timer/wakeup state | Clock / Timer Service |

Components may maintain indexes or references to state owned elsewhere.

Those indexes must not become competing sources of truth.

---

# Dependency Direction

Dependencies must flow toward the Domain.

Conceptually:

    CLI ───────────────┐
                       │
    IPC ───────────────┤
                       ▼
                    Broker
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
      Registries    Scheduler   Execution Manager
          │            │            │
          └────────────┼────────────┘
                       ▼
                     Domain
                       ▲
                       │
               Pure Policy Logic

Infrastructure adapters are invoked through boundaries:

    Execution Manager → Process Runner → OS

    Scheduler → Timer Service → System Clock

    Application → Event Sink → IPC / JSON / CLI

Domain logic must not depend back outward on these adapters.

---

# Request Flow

A normal Request proceeds as follows.

## 1. Receive

The IPC Server receives the Request and passes it to the Broker.

## 2. Register

The Broker registers the Request with the Request Registry.

## 3. Resolve identity

The Broker resolves its Key.

## 4. Resolve and bind Policy Scope

The Broker resolves:

    explicit Group → Group Policy Scope

or:

    no Group → Key Policy Scope

The Key Registry establishes the binding on first use or validates the existing binding.
A mismatched Group is rejected while the Key has active or pending work.

## 5. Update desired state

The Key Registry updates the Key's Execution Definition using last-wins semantics.

## 6. Evaluate contention

The Policy Scope's Contention Policy evaluates existing same-Key state and its
authoritative pending-work set.

It may decide to:

- attach
- request fresh execution
- pend
- supersede
- drop
- cancel and replace

## 7. Queue and admit fresh work

Surviving work that requires a fresh Execution is recorded in the Policy Scope's
pending-work set. The Pending Scheduler selects pending work from that Scope and the
Admission Controller evaluates it.

The selected Request may be admitted immediately, but it cannot bypass existing pending
work in the same Scope. If Admission is blocked, the Scheduler tracks when the Scope
should be reconsidered.

## 8. Create Execution

The Execution Manager snapshots the latest Execution Definition and creates an Execution.

The Execution enters its Policy Scope and becomes active for its Key.

## 9. Create Attempt

The Execution Manager requests that the Process Runner start the first Attempt.

## 10. Observe Attempt

The Process Runner emits output and eventually reports an Attempt Outcome.

## 11. Evaluate retry

The Execution Manager passes unsuccessful Attempt state to Retry Policy.

Retry Policy either completes the Execution or calculates the delay before another Attempt.

## 12. Retry if necessary

The Timer Service schedules the retry wakeup.

When eligible, the Execution Manager creates another Attempt.

## 13. Complete

Once no further Attempts are required, the Execution Manager produces the final Execution Outcome.

## 14. Propagate

The Request Registry propagates that Outcome to all Requests satisfied by the Execution.

## 15. Release capacity

The Execution leaves its Policy Scope and active Key state.

## 16. Reconsider pending work

The Pending Scheduler reacts to the state change and attempts to admit eligible pending work.

---

# Concurrency Model

Concurrency correctness should derive from ownership boundaries rather than distributed locking conventions.

## Key serialization

Operations that mutate the same Key's state must be serialized relative to each other.

This prevents races between:

- definition updates
- reuse decisions
- replacement
- Execution completion
- pending supersession

## Policy Scope serialization

Admission and capacity mutation within the same Policy Scope must be atomic relative to each other.

The system must never separately:

1. observe available capacity
2. asynchronously decide to start
3. later increment capacity

if another Request could perform the same sequence concurrently.

Admission and reservation of capacity are one logical operation.

## Execution isolation

Each Execution owns its own lifecycle.

Attempt completion and retry transitions for one Execution should not require global serialization with unrelated Executions.

The architecture should therefore permit high concurrency across independent Keys and Policy Scopes without introducing a single global execution lock.

---

# Failure Boundaries

Failures should remain local to the component responsible for the operation.

## Client failure

Client disconnection affects the Request relationship according to Request lifecycle semantics.

It does not inherently imply process failure.

## Process failure

Process failure produces an Attempt Outcome.

Retry Policy determines its Execution-level consequence.

## Retry exhaustion

Retry exhaustion produces a final Execution Outcome.

## IPC failure

Transport failure must not mutate execution semantics except where explicitly defined.

## Internal application failure

Registry or orchestration errors must be represented explicitly rather than silently converted into child-process failure.

An infrastructure failure and `COMMAND` exiting non-zero are different domain events.

---

# Testing Architecture

The architecture should support most behavior without spawning real processes or waiting on real time.

## Domain tests

Pure tests should cover:

- contention decisions
- admission decisions
- retry decisions
- backoff calculations
- jitter bounds
- Key and Policy Scope semantics

## Application tests

Use fake adapters for:

- Process Runner
- Clock / Timer
