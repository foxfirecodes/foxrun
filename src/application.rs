//! Serialized, deterministic application transactions for foxrun v2.
//!
//! This is intentionally a synchronous state machine.  Socket, process and timer
//! adapters send it commands; their side effects are returned as `Effect`s.

use std::collections::HashMap;
use std::time::Duration;

use crate::domain::*;
use crate::registries::{AdmissionReservation, KeyRegistry, PolicyScopeRegistry, RequestRegistry};

#[derive(Clone, Debug)]
pub struct SubmitRequest {
    pub key: Key,
    pub group: Option<GroupId>,
    pub definition: ExecutionDefinition,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubmitResult {
    pub request_id: RequestId,
    pub execution_id: Option<ExecutionId>,
}
/// A request that ended without ever belonging to an execution.  These still
/// need terminal delivery to request subscribers (for example `drop`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestTerminalEvent {
    pub request_id: RequestId,
    pub outcome: Outcome,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubscriptionReplay {
    Pending,
    Execution {
        execution_id: ExecutionId,
        replay: Vec<StreamEvent>,
    },
    Terminal {
        outcome: Outcome,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Effect {
    StartAttempt {
        execution_id: ExecutionId,
        attempt_id: AttemptId,
        definition: ExecutionDefinition,
    },
    CancelAttempt {
        execution_id: ExecutionId,
        attempt_id: AttemptId,
        kill_grace: Duration,
    },
    ScheduleAttemptTimeout {
        execution_id: ExecutionId,
        attempt_id: AttemptId,
        generation: u64,
        at: Duration,
    },
    ScheduleRetry {
        execution_id: ExecutionId,
        generation: u64,
        at: Duration,
    },
    ScheduleUnobservedGrace {
        execution_id: ExecutionId,
        generation: u64,
        at: Duration,
    },
    ScheduleAdmission {
        scope: PolicyScopeId,
        at: Duration,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LifecycleEvent {
    Request {
        request_id: RequestId,
        state: RequestState,
        outcome: Option<Outcome>,
    },
    Execution {
        execution_id: ExecutionId,
        state: ExecutionState,
        outcome: Option<Outcome>,
    },
    Attempt {
        execution_id: ExecutionId,
        attempt_id: AttemptId,
        outcome: Outcome,
    },
    Output {
        attempt_id: AttemptId,
        stream: crate::protocol::OutputStream,
        data: Vec<u8>,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamEvent {
    pub sequence: u64,
    pub request_id: Option<RequestId>,
    pub event: LifecycleEvent,
}

/// A single-owner actor state. Callers must serialize calls (the Tokio adapter
/// does this with a single command channel); tests call it directly.
pub struct Application {
    next_execution: u64,
    next_attempt: u64,
    requests: RequestRegistry,
    keys: KeyRegistry,
    scopes: PolicyScopeRegistry,
    executions: HashMap<ExecutionId, Execution>,
    attempts: HashMap<AttemptId, Attempt>,
    streams: HashMap<ExecutionId, Vec<StreamEvent>>,
    effects: Vec<Effect>,
    retry_generation: HashMap<ExecutionId, u64>,
    unobserved_generation: HashMap<ExecutionId, u64>,
    timeout_generation: HashMap<AttemptId, u64>,
    forced_outcomes: HashMap<AttemptId, Outcome>,
    request_terminals: Vec<RequestTerminalEvent>,
}

impl Default for Application {
    fn default() -> Self {
        Self::new()
    }
}
impl Application {
    pub fn new() -> Self {
        Self {
            next_execution: 1,
            next_attempt: 1,
            requests: RequestRegistry::default(),
            keys: KeyRegistry::default(),
            scopes: PolicyScopeRegistry::default(),
            executions: HashMap::new(),
            attempts: HashMap::new(),
            streams: HashMap::new(),
            effects: vec![],
            retry_generation: HashMap::new(),
            unobserved_generation: HashMap::new(),
            timeout_generation: HashMap::new(),
            forced_outcomes: HashMap::new(),
            request_terminals: vec![],
        }
    }
    pub fn configure_scope(&mut self, id: PolicyScopeId, policy: ScopePolicy) {
        self.scopes.ensure_scope(id.clone());
        self.scopes
            .configure(&id, policy)
            .expect("scope was ensured");
    }
    /// Applies only the scope settings explicitly supplied by a client. This
    /// prevents a later bare submission from resetting an established group.
    pub fn configure_scope_patch(
        &mut self,
        id: PolicyScopeId,
        contention: Option<ContentionMode>,
        max_concurrency: Option<Option<usize>>,
        rate_limit: Option<Option<RateLimit>>,
    ) {
        self.scopes.ensure_scope(id.clone());
        let mut policy = self.scopes.policy(&id).expect("scope was ensured").clone();
        if let Some(value) = contention {
            policy.contention = value;
        }
        if let Some(value) = max_concurrency {
            policy.admission.max_concurrency = value;
        }
        if let Some(value) = rate_limit {
            policy.admission.rate_limit = value;
        }
        self.scopes
            .configure(&id, policy)
            .expect("scope was ensured");
    }
    pub fn request_state(&self, id: RequestId) -> Option<RequestState> {
        self.requests.get(id).map(|r| r.state)
    }
    pub fn execution(&self, id: ExecutionId) -> Option<&Execution> {
        self.executions.get(&id)
    }
    /// Correlation lookup for transport event envelopes. Request state remains
    /// authoritative here; adapters only receive stable IDs.
    pub fn requests_for_execution(&self, execution: ExecutionId) -> Vec<RequestId> {
        self.requests
            .values()
            .filter_map(|request| {
                (request.execution_id == Some(execution)).then_some(request.request.id)
            })
            .collect()
    }
    pub fn take_effects(&mut self) -> Vec<Effect> {
        std::mem::take(&mut self.effects)
    }
    pub fn take_request_terminals(&mut self) -> Vec<RequestTerminalEvent> {
        std::mem::take(&mut self.request_terminals)
    }
    pub fn events_since(&self, execution: ExecutionId, cursor: u64) -> Vec<StreamEvent> {
        self.streams
            .get(&execution)
            .into_iter()
            .flatten()
            .filter(|e| e.sequence > cursor)
            .cloned()
            .collect()
    }
    fn event(&mut self, execution: ExecutionId, event: LifecycleEvent) {
        let request_id = self.requests.values().find_map(|request| {
            (request.execution_id == Some(execution)).then_some(request.request.id)
        });
        let stream = self.streams.entry(execution).or_default();
        let sequence = stream.last().map_or(1, |event| event.sequence + 1);
        stream.push(StreamEvent {
            sequence,
            request_id,
            event,
        });
    }
    /// Records adapter output in the same ordered stream as lifecycle facts.
    /// Returns false when the attempt is stale or no longer active.
    pub fn record_output(
        &mut self,
        execution: ExecutionId,
        attempt: AttemptId,
        stream: crate::protocol::OutputStream,
        data: Vec<u8>,
    ) -> bool {
        if self
            .executions
            .get(&execution)
            .is_none_or(|e| e.attempts.last() != Some(&attempt) || e.state.is_terminal())
        {
            return false;
        }
        self.event(
            execution,
            LifecycleEvent::Output {
                attempt_id: attempt,
                stream,
                data,
            },
        );
        true
    }

    pub fn submit(&mut self, now: Duration, input: SubmitRequest) -> SubmitResult {
        let scope = PolicyScopeId::for_key(&input.key, input.group);
        // Different scope is rejected only while key work remains live/pending.
        if let Some(bound) = self.keys.get(&input.key).map(|record| record.scope.clone()) {
            if bound != scope
                && (self.keys.active_execution(&input.key).is_some()
                    || self
                        .requests
                        .values()
                        .any(|r| r.request.key == input.key && r.state == RequestState::Pending))
            {
                let id = self
                    .requests
                    .register(input.key, scope, input.definition, now);
                self.requests
                    .reject(id, "key is bound to another active scope")
                    .unwrap();
                self.request_terminals.push(RequestTerminalEvent {
                    request_id: id,
                    outcome: Outcome::Rejected {
                        reason: "key is bound to another active scope".into(),
                    },
                });
                return SubmitResult {
                    request_id: id,
                    execution_id: None,
                };
            }
        }
        self.scopes.ensure_scope(scope.clone());
        self.keys
            .upsert_definition(input.key.clone(), scope.clone(), input.definition.clone())
            .unwrap();
        let id = self
            .requests
            .register(input.key.clone(), scope.clone(), input.definition, now);
        let active = self.keys.active_execution(&input.key);
        let pending: Vec<_> = self
            .scopes
            .get(&scope)
            .unwrap()
            .pending
            .iter()
            .filter_map(|((_, request), key)| (key == &input.key).then_some(*request))
            .collect();
        let s = self.scopes.get(&scope).unwrap();
        let can = matches!(
            evaluate_admission(
                &s.policy.admission,
                AdmissionState {
                    active_permits: s.permits.len(),
                    admissions: &s.admissions
                },
                now
            ),
            AdmissionDecision::Admit
        ) && pending.is_empty()
            && active.is_none();
        match decide_contention(ContentionContext {
            mode: s.policy.contention,
            active_execution: active,
            pending_for_key: &pending,
            can_start_fresh: can,
        }) {
            ContentionDecision::Attach(e) => {
                self.attach(id, e);
            }
            ContentionDecision::Drop => {
                self.requests
                    .drop(id, "contention policy dropped request")
                    .unwrap();
                self.request_terminals.push(RequestTerminalEvent {
                    request_id: id,
                    outcome: Outcome::Dropped {
                        reason: "contention policy dropped request".into(),
                    },
                });
            }
            ContentionDecision::Replace(e) => {
                self.pend(id);
                self.cancel_execution(e);
            }
            ContentionDecision::Pend | ContentionDecision::SupersedePendingAndPend(_) => {
                for old in pending {
                    self.supersede(old, id);
                }
                self.pend(id);
            }
        }
        self.reconsider(now, &scope);
        SubmitResult {
            request_id: id,
            execution_id: self.requests.get(id).unwrap().execution_id,
        }
    }
    fn pend(&mut self, id: RequestId) {
        let r = self.requests.get(id).unwrap();
        let scope = r.request.scope.clone();
        let when = r.request.received_at;
        let key = r.request.key.clone();
        self.requests.pend(id).unwrap();
        self.scopes
            .add_pending(&scope, key.clone(), id, when)
            .unwrap();
        self.keys.increment_pending(&key).unwrap();
    }
    fn attach(&mut self, id: RequestId, e: ExecutionId) {
        self.requests.attach(id, e).unwrap();
    }
    fn supersede(&mut self, id: RequestId, by: RequestId) {
        if let Some(r) = self.requests.get(id) {
            if r.state == RequestState::Pending {
                let scope = r.request.scope.clone();
                let key = r.request.key.clone();
                self.scopes.remove_pending(&scope, id).unwrap();
                self.keys.decrement_pending(&key).unwrap();
                self.requests.supersede(id, by).unwrap();
                self.request_terminals.push(RequestTerminalEvent {
                    request_id: id,
                    outcome: Outcome::Superseded { by },
                });
            }
        }
    }
    fn reconsider(&mut self, now: Duration, scope: &PolicyScopeId) {
        loop {
            let selected = {
                let s = self.scopes.get(scope).unwrap();
                s.pending.iter().find_map(|((_, id), key)| {
                    self.keys.active_execution(key).is_none().then_some(*id)
                })
            };
            let Some(id) = selected else { return };
            let decision = {
                let s = self.scopes.get(scope).unwrap();
                evaluate_admission(
                    &s.policy.admission,
                    AdmissionState {
                        active_permits: s.permits.len(),
                        admissions: &s.admissions,
                    },
                    now,
                )
            };
            match decision {
                AdmissionDecision::Admit => self.start(now, id),
                AdmissionDecision::BlockOnCapacity => return,
                AdmissionDecision::BlockUntil(at) => {
                    self.effects.push(Effect::ScheduleAdmission {
                        scope: scope.clone(),
                        at,
                    });
                    return;
                }
            }
        }
    }
    pub fn reconsider_scope(&mut self, now: Duration, scope: PolicyScopeId) {
        self.reconsider(now, &scope);
    }
    fn start(&mut self, now: Duration, id: RequestId) {
        let (key, scope) = {
            let r = self.requests.get(id).unwrap();
            (r.request.key.clone(), r.request.scope.clone())
        };
        self.scopes.remove_pending(&scope, id).unwrap();
        self.keys.decrement_pending(&key).unwrap();
        let permit = match self.scopes.try_reserve(&scope, now).unwrap() {
            AdmissionReservation::Reserved(permit) => permit,
            AdmissionReservation::BlockedOnCapacity | AdmissionReservation::BlockedUntil(_) => {
                panic!("admission changed inside the serialized actor")
            }
        };
        let (definition, version) = self.keys.current_definition(&key).unwrap();
        let definition = definition.clone();
        let eid = ExecutionId(self.next_execution);
        self.next_execution += 1;
        let aid = AttemptId(self.next_attempt);
        self.next_attempt += 1;
        self.executions.insert(
            eid,
            Execution {
                id: eid,
                key: key.clone(),
                scope: scope.clone(),
                definition_version: version,
                definition: definition.clone(),
                permit,
                state: ExecutionState::Running,
                attempts: vec![aid],
                outcome: None,
            },
        );
        self.attempts.insert(
            aid,
            Attempt {
                id: aid,
                execution_id: eid,
                state: AttemptState::Running,
                outcome: None,
            },
        );
        self.keys.set_active(&key, eid).unwrap();
        self.requests.assign(id, eid).unwrap();
        self.effects.push(Effect::StartAttempt {
            execution_id: eid,
            attempt_id: aid,
            definition: definition.clone(),
        });
        self.schedule_attempt_timeout(now, eid, aid, &definition);
        self.event(
            eid,
            LifecycleEvent::Execution {
                execution_id: eid,
                state: ExecutionState::Running,
                outcome: None,
            },
        );
    }
    pub fn complete_attempt(
        &mut self,
        now: Duration,
        execution: ExecutionId,
        attempt: AttemptId,
        outcome: Outcome,
    ) -> bool {
        let Some(e) = self.executions.get(&execution) else {
            return false;
        };
        if (e.state != ExecutionState::Running && e.state != ExecutionState::Cancelling)
            || e.attempts.last() != Some(&attempt)
        {
            return false;
        }
        let Some(a) = self.attempts.get_mut(&attempt) else {
            return false;
        };
        if a.state.is_terminal() {
            return false;
        };
        let outcome = self.forced_outcomes.remove(&attempt).unwrap_or(outcome);
        a.state = match outcome {
            Outcome::Succeeded => AttemptState::Succeeded,
            Outcome::TimedOut => AttemptState::TimedOut,
            Outcome::Cancelled => AttemptState::Cancelled,
            _ => AttemptState::Failed,
        };
        a.outcome = Some(outcome.clone());
        self.event(
            execution,
            LifecycleEvent::Attempt {
                execution_id: execution,
                attempt_id: attempt,
                outcome: outcome.clone(),
            },
        );
        let attempts = self.executions[&execution].attempts.len() as u32;
        match decide_retry(
            &self.executions[&execution].definition.retry,
            attempts,
            &outcome,
            0,
        ) {
            RetryDecision::RetryAfter(delay) => {
                self.executions.get_mut(&execution).unwrap().state = ExecutionState::RetryWaiting;
                let g = self.retry_generation.entry(execution).or_insert(0);
                *g += 1;
                self.effects.push(Effect::ScheduleRetry {
                    execution_id: execution,
                    generation: *g,
                    at: now.saturating_add(delay),
                });
            }
            RetryDecision::Complete => self.finish(now, execution, outcome),
        };
        true
    }
    /// The adapter calls this when the immutable attempt timeout elapses.  It
    /// records the intended outcome first, then asks the adapter to terminate
    /// the process; the eventual OS exit commits the terminal transition.
    pub fn attempt_timeout_expired(
        &mut self,
        execution: ExecutionId,
        attempt: AttemptId,
        generation: u64,
    ) -> bool {
        if self.timeout_generation.get(&attempt).copied() != Some(generation)
            || self.executions.get(&execution).is_none_or(|e| {
                e.state != ExecutionState::Running || e.attempts.last() != Some(&attempt)
            })
        {
            return false;
        }
        let kill_grace = self.executions[&execution].definition.kill_grace;
        self.executions.get_mut(&execution).unwrap().state = ExecutionState::Cancelling;
        self.forced_outcomes.insert(attempt, Outcome::TimedOut);
        self.effects.push(Effect::CancelAttempt {
            execution_id: execution,
            attempt_id: attempt,
            kill_grace,
        });
        self.event(
            execution,
            LifecycleEvent::Execution {
                execution_id: execution,
                state: ExecutionState::Cancelling,
                outcome: None,
            },
        );
        true
    }
    pub fn retry_due(&mut self, _now: Duration, execution: ExecutionId, generation: u64) -> bool {
        if self.retry_generation.get(&execution).copied() != Some(generation)
            || self.executions.get(&execution).map(|e| e.state)
                != Some(ExecutionState::RetryWaiting)
        {
            return false;
        };
        let aid = AttemptId(self.next_attempt);
        self.next_attempt += 1;
        let definition = {
            let e = self.executions.get_mut(&execution).unwrap();
            e.state = ExecutionState::Running;
            e.attempts.push(aid);
            e.definition.clone()
        };
        self.attempts.insert(
            aid,
            Attempt {
                id: aid,
                execution_id: execution,
                state: AttemptState::Running,
                outcome: None,
            },
        );
        self.effects.push(Effect::StartAttempt {
            execution_id: execution,
            attempt_id: aid,
            definition: definition.clone(),
        });
        self.schedule_attempt_timeout(_now, execution, aid, &definition);
        true
    }
    pub fn cancel_request(&mut self, now: Duration, id: RequestId) -> bool {
        let Some(r) = self.requests.get(id) else {
            return false;
        };
        match r.state {
            RequestState::Pending => {
                let scope = r.request.scope.clone();
                let key = r.request.key.clone();
                self.scopes.remove_pending(&scope, id).unwrap();
                self.keys.decrement_pending(&key).unwrap();
                self.requests.cancel(id).unwrap();
                self.request_terminals.push(RequestTerminalEvent {
                    request_id: id,
                    outcome: Outcome::Cancelled,
                });
                true
            }
            RequestState::Attached | RequestState::Assigned => {
                if let Some(e) = r.execution_id {
                    self.cancel_execution(e);
                }
                let _ = now;
                true
            }
            _ => false,
        }
    }
    fn cancel_execution(&mut self, e: ExecutionId) {
        let Some(ex) = self.executions.get_mut(&e) else {
            return;
        };
        if ex.state.is_terminal() {
            return;
        }
        ex.state = ExecutionState::Cancelling;
        let kill_grace = ex.definition.kill_grace;
        if let Some(a) = ex.attempts.last().copied() {
            self.forced_outcomes.insert(a, Outcome::Cancelled);
            self.effects.push(Effect::CancelAttempt {
                execution_id: e,
                attempt_id: a,
                kill_grace,
            });
        }
    }
    fn finish(&mut self, now: Duration, e: ExecutionId, outcome: Outcome) {
        let Some(ex) = self.executions.get_mut(&e) else {
            return;
        };
        if ex.state.is_terminal() {
            return;
        };
        ex.state = outcome
            .execution_state()
            .unwrap_or(ExecutionState::Cancelled);
        ex.outcome = Some(outcome.clone());
        let key = ex.key.clone();
        let scope = ex.scope.clone();
        let permit = ex.permit;
        self.keys.clear_active(&key, e).unwrap();
        self.scopes.release(&scope, permit).unwrap();
        let completed: Vec<_> = self
            .requests
            .values()
            .filter_map(|r| {
                (r.execution_id == Some(e) && !r.state.is_terminal()).then_some(r.request.id)
            })
            .collect();
        for id in &completed {
            self.requests.complete(*id, outcome.clone()).unwrap();
        }
        self.event(
            e,
            LifecycleEvent::Execution {
                execution_id: e,
                state: self.executions[&e].state,
                outcome: Some(outcome.clone()),
            },
        );
        for request_id in completed {
            self.event(
                e,
                LifecycleEvent::Request {
                    request_id,
                    state: outcome.request_state(),
                    outcome: Some(outcome.clone()),
                },
            );
        }
        self.reconsider(now, &scope);
    }
    pub fn disconnect(&mut self, now: Duration, subscription: SubscriptionId) {
        for r in self.requests.values_mut() {
            r.subscriptions.remove(&subscription);
        }
        let unobserved: Vec<_> = self
            .executions
            .iter()
            .filter_map(|(id, execution)| {
                (!execution.state.is_terminal()
                    && !self.requests.values().any(|request| {
                        request.execution_id == Some(*id) && !request.subscriptions.is_empty()
                    }))
                .then_some((*id, execution.definition.unobserved_grace))
            })
            .collect();
        for (id, grace) in unobserved {
            if let Some(grace) = grace {
                let generation = self.unobserved_generation.entry(id).or_insert(0);
                *generation += 1;
                self.effects.push(Effect::ScheduleUnobservedGrace {
                    execution_id: id,
                    generation: *generation,
                    at: now.saturating_add(grace),
                });
            }
        }
    }
    pub fn unobserved_grace_expired(&mut self, execution: ExecutionId, generation: u64) -> bool {
        if self.unobserved_generation.get(&execution).copied() != Some(generation)
            || self.requests.values().any(|request| {
                request.execution_id == Some(execution) && !request.subscriptions.is_empty()
            })
        {
            return false;
        }
        self.cancel_execution(execution);
        true
    }
    pub fn subscribe(
        &mut self,
        request: RequestId,
        subscription: SubscriptionId,
    ) -> Option<ExecutionId> {
        let r = self.requests.get_mut(request)?;
        r.subscriptions.insert(subscription);
        let execution = r.execution_id?;
        self.unobserved_generation
            .entry(execution)
            .and_modify(|generation| *generation += 1)
            .or_insert(1);
        Some(execution)
    }
    /// Atomically register a subscription and obtain retained events strictly
    /// after `cursor`; callers deliver this vector before subsequent live data.
    pub fn subscribe_with_replay(
        &mut self,
        request: RequestId,
        subscription: SubscriptionId,
        cursor: u64,
    ) -> Option<SubscriptionReplay> {
        // A subscription is to a *request*, not merely an already-created
        // execution.  In particular, a queued request must be observable
        // before admission creates its execution.
        self.requests.subscribe(request, subscription).ok()?;
        let record = self.requests.get(request)?;
        if record.state.is_terminal() && record.execution_id.is_none() {
            return record
                .outcome
                .clone()
                .map(|outcome| SubscriptionReplay::Terminal { outcome });
        }
        let execution = record.execution_id;
        if let Some(execution) = execution {
            self.unobserved_generation
                .entry(execution)
                .and_modify(|generation| *generation += 1)
                .or_insert(1);
            Some(SubscriptionReplay::Execution {
                execution_id: execution,
                replay: self.events_since(execution, cursor),
            })
        } else {
            Some(SubscriptionReplay::Pending)
        }
    }

    fn schedule_attempt_timeout(
        &mut self,
        now: Duration,
        execution: ExecutionId,
        attempt: AttemptId,
        definition: &ExecutionDefinition,
    ) {
        let Some(timeout) = definition.attempt_timeout else {
            return;
        };
        let generation = self.timeout_generation.entry(attempt).or_insert(0);
        *generation += 1;
        self.effects.push(Effect::ScheduleAttemptTimeout {
            execution_id: execution,
            attempt_id: attempt,
            generation: *generation,
            at: now.saturating_add(timeout),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn def() -> ExecutionDefinition {
        ExecutionDefinition {
            command: Command {
                executable: "x".into(),
                arguments: vec![],
                working_directory: None,
            },
            retry: RetryPolicy::default(),
            attempt_timeout: None,
            kill_grace: Duration::ZERO,
            unobserved_grace: Some(Duration::from_secs(1)),
        }
    }
    fn sub(key: &str) -> SubmitRequest {
        SubmitRequest {
            key: Key::from(key),
            group: None,
            definition: def(),
        }
    }
    #[test]
    fn submit_snapshots_latest_and_rejects_stale_completion() {
        let mut a = Application::new();
        let one = a.submit(Duration::ZERO, sub("k"));
        let e = one.execution_id.unwrap();
        let old = a.execution(e).unwrap().definition_version;
        let two = a.submit(Duration::from_secs(1), sub("k"));
        assert_eq!(a.execution(e).unwrap().definition_version, old);
        let attempt = a.execution(e).unwrap().attempts[0];
        assert!(a.complete_attempt(Duration::from_secs(2), e, attempt, Outcome::Succeeded));
        assert!(!a.complete_attempt(Duration::from_secs(3), e, attempt, Outcome::Succeeded));
        assert_eq!(
            a.request_state(two.request_id),
            Some(RequestState::Succeeded)
        );
    }
    #[test]
    fn group_oldest_runnable_and_atomic_permit() {
        let mut a = Application::new();
        let g = GroupId::from("g");
        let scope = PolicyScopeId::Group(g.clone());
        a.configure_scope(
            scope,
            ScopePolicy {
                contention: ContentionMode::Queue,
                admission: AdmissionPolicy {
                    max_concurrency: Some(1),
                    rate_limit: None,
                },
            },
        );
        let mut x = sub("a");
        x.group = Some(g.clone());
        let mut y = sub("b");
        y.group = Some(g);
        let r1 = a.submit(Duration::ZERO, x);
        let r2 = a.submit(Duration::from_secs(1), y);
        assert!(r1.execution_id.is_some());
        assert_eq!(a.request_state(r2.request_id), Some(RequestState::Pending));
        let e = r1.execution_id.unwrap();
        let at = a.execution(e).unwrap().attempts[0];
        a.complete_attempt(Duration::from_secs(2), e, at, Outcome::Succeeded);
        assert_eq!(a.request_state(r2.request_id), Some(RequestState::Assigned));
    }

    #[test]
    fn pending_request_can_subscribe_before_admission() {
        let mut a = Application::new();
        let group = GroupId::from("g");
        a.configure_scope(
            PolicyScopeId::Group(group.clone()),
            ScopePolicy {
                contention: ContentionMode::Queue,
                admission: AdmissionPolicy {
                    max_concurrency: Some(1),
                    rate_limit: None,
                },
            },
        );
        let mut first = sub("first");
        first.group = Some(group.clone());
        let mut pending = sub("pending");
        pending.group = Some(group);
        let first = a.submit(Duration::ZERO, first);
        let pending = a.submit(Duration::ZERO, pending);

        assert_eq!(
            a.subscribe_with_replay(pending.request_id, SubscriptionId(9), 0),
            Some(SubscriptionReplay::Pending)
        );

        let execution = first.execution_id.unwrap();
        let attempt = a.execution(execution).unwrap().attempts[0];
        assert!(a.complete_attempt(Duration::ZERO, execution, attempt, Outcome::Succeeded));
        let assigned = a
            .execution(
                a.requests
                    .get(pending.request_id)
                    .unwrap()
                    .execution_id
                    .unwrap(),
            )
            .unwrap()
            .id;
        assert!(matches!(
            a.subscribe_with_replay(pending.request_id, SubscriptionId(9), 0),
            Some(SubscriptionReplay::Execution { execution_id: id, .. }) if id == assigned
        ));
    }

    #[test]
    fn dropped_request_replays_a_terminal_outcome_without_an_execution() {
        let mut a = Application::new();
        let group = GroupId::from("g");
        a.configure_scope(
            PolicyScopeId::Group(group.clone()),
            ScopePolicy {
                contention: ContentionMode::Drop,
                admission: AdmissionPolicy {
                    max_concurrency: Some(1),
                    rate_limit: None,
                },
            },
        );
        let mut first = sub("first");
        first.group = Some(group.clone());
        let mut dropped = sub("dropped");
        dropped.group = Some(group);
        a.submit(Duration::ZERO, first);
        let dropped = a.submit(Duration::ZERO, dropped);

        assert!(matches!(
            a.subscribe_with_replay(dropped.request_id, SubscriptionId(1), 0),
            Some(SubscriptionReplay::Terminal {
                outcome: Outcome::Dropped { .. }
            })
        ));
    }

    #[test]
    fn cancellation_commits_when_the_process_reports_an_ordinary_exit() {
        let mut a = Application::new();
        let submitted = a.submit(Duration::ZERO, sub("k"));
        let execution = submitted.execution_id.unwrap();
        let attempt = a.execution(execution).unwrap().attempts[0];
        assert!(a.cancel_request(Duration::ZERO, submitted.request_id));
        assert!(a.complete_attempt(
            Duration::ZERO,
            execution,
            attempt,
            Outcome::Failed {
                exit_code: None,
                signal: None,
            },
        ));
        assert_eq!(
            a.request_state(submitted.request_id),
            Some(RequestState::Cancelled)
        );
        assert_eq!(
            a.execution(execution).unwrap().outcome,
            Some(Outcome::Cancelled)
        );
    }

    #[test]
    fn timeout_uses_generation_and_commits_after_process_exit() {
        let mut a = Application::new();
        let mut request = sub("k");
        request.definition.attempt_timeout = Some(Duration::from_secs(1));
        let submitted = a.submit(Duration::ZERO, request);
        let execution = submitted.execution_id.unwrap();
        let attempt = a.execution(execution).unwrap().attempts[0];
        let timeout = a
            .take_effects()
            .into_iter()
            .find_map(|effect| match effect {
                Effect::ScheduleAttemptTimeout { generation, .. } => Some(generation),
                _ => None,
            })
            .unwrap();
        assert!(a.attempt_timeout_expired(execution, attempt, timeout));
        assert!(a.complete_attempt(
            Duration::from_secs(1),
            execution,
            attempt,
            Outcome::Failed {
                exit_code: None,
                signal: None,
            },
        ));
        assert_eq!(
            a.request_state(submitted.request_id),
            Some(RequestState::TimedOut)
        );
    }
}
