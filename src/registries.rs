//! Authoritative, synchronous state owners used by the serialized application actor.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use crate::domain::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegistryError {
    UnknownRequest(RequestId),
    UnknownKey(Key),
    InvalidRequestTransition {
        from: RequestState,
        to: RequestState,
    },
    RequestAlreadyBound(RequestId),
    ScopeBindingConflict {
        key: Key,
        bound: PolicyScopeId,
        requested: PolicyScopeId,
    },
    UnknownScope(PolicyScopeId),
    UnknownPermit(AdmissionPermitId),
    PermitAlreadyReleased(AdmissionPermitId),
}

#[derive(Clone, Debug)]
pub struct RequestRecord {
    pub request: Request,
    pub state: RequestState,
    pub execution_id: Option<ExecutionId>,
    pub outcome: Option<Outcome>,
    pub superseded_by: Option<RequestId>,
    pub subscriptions: BTreeSet<SubscriptionId>,
}

#[derive(Default)]
pub struct RequestRegistry {
    next_id: u64,
    records: BTreeMap<RequestId, RequestRecord>,
}
impl RequestRegistry {
    pub fn register(
        &mut self,
        key: Key,
        scope: PolicyScopeId,
        definition: ExecutionDefinition,
        received_at: Duration,
    ) -> RequestId {
        self.next_id += 1;
        let id = RequestId(self.next_id);
        self.records.insert(
            id,
            RequestRecord {
                request: Request {
                    id,
                    key,
                    scope,
                    definition,
                    received_at,
                },
                state: RequestState::Received,
                execution_id: None,
                outcome: None,
                superseded_by: None,
                subscriptions: BTreeSet::new(),
            },
        );
        id
    }
    pub fn get(&self, id: RequestId) -> Option<&RequestRecord> {
        self.records.get(&id)
    }
    pub(crate) fn get_mut(&mut self, id: RequestId) -> Option<&mut RequestRecord> {
        self.records.get_mut(&id)
    }
    pub(crate) fn values(&self) -> impl Iterator<Item = &RequestRecord> {
        self.records.values()
    }
    pub(crate) fn values_mut(&mut self) -> impl Iterator<Item = &mut RequestRecord> {
        self.records.values_mut()
    }
    pub fn request(&self, id: RequestId) -> Result<&Request, RegistryError> {
        self.get(id)
            .map(|r| &r.request)
            .ok_or(RegistryError::UnknownRequest(id))
    }
    pub fn attach(&mut self, id: RequestId, execution: ExecutionId) -> Result<(), RegistryError> {
        self.bind(id, execution, RequestState::Attached)
    }
    pub fn assign(&mut self, id: RequestId, execution: ExecutionId) -> Result<(), RegistryError> {
        self.bind(id, execution, RequestState::Assigned)
    }
    fn bind(
        &mut self,
        id: RequestId,
        execution: ExecutionId,
        state: RequestState,
    ) -> Result<(), RegistryError> {
        let r = self
            .records
            .get_mut(&id)
            .ok_or(RegistryError::UnknownRequest(id))?;
        if r.state != RequestState::Received && r.state != RequestState::Pending {
            return Err(RegistryError::InvalidRequestTransition {
                from: r.state,
                to: state,
            });
        }
        if r.execution_id.is_some() {
            return Err(RegistryError::RequestAlreadyBound(id));
        }
        r.execution_id = Some(execution);
        r.state = state;
        Ok(())
    }
    pub fn pend(&mut self, id: RequestId) -> Result<(), RegistryError> {
        self.transition(id, RequestState::Pending)
    }
    pub fn supersede(&mut self, id: RequestId, by: RequestId) -> Result<(), RegistryError> {
        let r = self
            .records
            .get_mut(&id)
            .ok_or(RegistryError::UnknownRequest(id))?;
        if !matches!(r.state, RequestState::Received | RequestState::Pending) {
            return Err(RegistryError::InvalidRequestTransition {
                from: r.state,
                to: RequestState::Superseded,
            });
        }
        r.state = RequestState::Superseded;
        r.outcome = Some(Outcome::Superseded { by });
        r.superseded_by = Some(by);
        Ok(())
    }
    pub fn drop(&mut self, id: RequestId, reason: impl Into<String>) -> Result<(), RegistryError> {
        self.complete(
            id,
            Outcome::Dropped {
                reason: reason.into(),
            },
        )
    }
    pub fn reject(
        &mut self,
        id: RequestId,
        reason: impl Into<String>,
    ) -> Result<(), RegistryError> {
        self.complete(
            id,
            Outcome::Rejected {
                reason: reason.into(),
            },
        )
    }
    pub fn cancel(&mut self, id: RequestId) -> Result<(), RegistryError> {
        self.complete(id, Outcome::Cancelled)
    }
    pub fn complete(&mut self, id: RequestId, outcome: Outcome) -> Result<(), RegistryError> {
        let r = self
            .records
            .get_mut(&id)
            .ok_or(RegistryError::UnknownRequest(id))?;
        if r.state.is_terminal() {
            return Err(RegistryError::InvalidRequestTransition {
                from: r.state,
                to: outcome.request_state(),
            });
        }
        if !matches!(
            r.state,
            RequestState::Attached
                | RequestState::Assigned
                | RequestState::Pending
                | RequestState::Received
        ) {
            return Err(RegistryError::InvalidRequestTransition {
                from: r.state,
                to: outcome.request_state(),
            });
        }
        r.state = outcome.request_state();
        r.outcome = Some(outcome);
        Ok(())
    }
    fn transition(&mut self, id: RequestId, to: RequestState) -> Result<(), RegistryError> {
        let r = self
            .records
            .get_mut(&id)
            .ok_or(RegistryError::UnknownRequest(id))?;
        if r.state != RequestState::Received {
            return Err(RegistryError::InvalidRequestTransition { from: r.state, to });
        }
        r.state = to;
        Ok(())
    }
    pub fn subscribe(
        &mut self,
        id: RequestId,
        subscriber: SubscriptionId,
    ) -> Result<(), RegistryError> {
        self.records
            .get_mut(&id)
            .ok_or(RegistryError::UnknownRequest(id))?
            .subscriptions
            .insert(subscriber);
        Ok(())
    }
    pub fn unsubscribe(&mut self, subscriber: SubscriptionId) {
        for r in self.records.values_mut() {
            r.subscriptions.remove(&subscriber);
        }
    }
    pub fn requests_for_execution(&self, execution: ExecutionId) -> Vec<RequestId> {
        self.records
            .iter()
            .filter_map(|(id, r)| (r.execution_id == Some(execution)).then_some(*id))
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct KeyRecord {
    pub scope: PolicyScopeId,
    pub definition: ExecutionDefinition,
    pub definition_version: DefinitionVersion,
    pub active_execution: Option<ExecutionId>,
    pub pending_count: usize,
}
#[derive(Default)]
pub struct KeyRegistry {
    records: BTreeMap<Key, KeyRecord>,
    next_definition: u64,
}
impl KeyRegistry {
    pub fn bind_scope(&mut self, key: Key, scope: PolicyScopeId) -> Result<(), RegistryError> {
        match self.records.get_mut(&key) {
            Some(record)
                if record.scope != scope
                    && (record.active_execution.is_some() || record.pending_count != 0) =>
            {
                Err(RegistryError::ScopeBindingConflict {
                    key,
                    bound: record.scope.clone(),
                    requested: scope,
                })
            }
            Some(record) => {
                record.scope = scope;
                Ok(())
            }
            None => Err(RegistryError::UnknownKey(key)),
        }
    }
    /// Creates a Key on first submission; later calls are last-wins definition updates.
    pub fn upsert_definition(
        &mut self,
        key: Key,
        scope: PolicyScopeId,
        definition: ExecutionDefinition,
    ) -> Result<DefinitionVersion, RegistryError> {
        self.next_definition += 1;
        let v = DefinitionVersion(self.next_definition);
        match self.records.get_mut(&key) {
            Some(r) => {
                if r.scope != scope && (r.active_execution.is_some() || r.pending_count != 0) {
                    return Err(RegistryError::ScopeBindingConflict {
                        key,
                        bound: r.scope.clone(),
                        requested: scope,
                    });
                }
                r.scope = scope;
                r.definition = definition;
                r.definition_version = v;
            }
            None => {
                self.records.insert(
                    key,
                    KeyRecord {
                        scope,
                        definition,
                        definition_version: v,
                        active_execution: None,
                        pending_count: 0,
                    },
                );
            }
        }
        Ok(v)
    }
    pub fn get(&self, key: &Key) -> Option<&KeyRecord> {
        self.records.get(key)
    }
    pub fn current_definition(
        &self,
        key: &Key,
    ) -> Result<(&ExecutionDefinition, DefinitionVersion), RegistryError> {
        let r = self
            .get(key)
            .ok_or_else(|| RegistryError::UnknownKey(key.clone()))?;
        Ok((&r.definition, r.definition_version))
    }
    pub fn active_execution(&self, key: &Key) -> Option<ExecutionId> {
        self.get(key).and_then(|r| r.active_execution)
    }
    pub fn set_active(&mut self, key: &Key, execution: ExecutionId) -> Result<(), RegistryError> {
        let r = self
            .records
            .get_mut(key)
            .ok_or_else(|| RegistryError::UnknownKey(key.clone()))?;
        r.active_execution = Some(execution);
        Ok(())
    }
    /// Compare-by-ID clearing rejects stale execution completion.
    pub fn clear_active(
        &mut self,
        key: &Key,
        execution: ExecutionId,
    ) -> Result<bool, RegistryError> {
        let r = self
            .records
            .get_mut(key)
            .ok_or_else(|| RegistryError::UnknownKey(key.clone()))?;
        if r.active_execution == Some(execution) {
            r.active_execution = None;
            Ok(true)
        } else {
            Ok(false)
        }
    }
    pub fn increment_pending(&mut self, key: &Key) -> Result<(), RegistryError> {
        let r = self
            .records
            .get_mut(key)
            .ok_or_else(|| RegistryError::UnknownKey(key.clone()))?;
        r.pending_count += 1;
        Ok(())
    }
    pub fn decrement_pending(&mut self, key: &Key) -> Result<(), RegistryError> {
        let r = self
            .records
            .get_mut(key)
            .ok_or_else(|| RegistryError::UnknownKey(key.clone()))?;
        r.pending_count = r.pending_count.saturating_sub(1);
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ScopeRecord {
    pub(crate) policy: ScopePolicy,
    pub(crate) pending: BTreeMap<(Duration, RequestId), Key>,
    pub(crate) admissions: Vec<Duration>,
    pub(crate) permits: BTreeSet<AdmissionPermitId>,
}
#[derive(Default)]
pub struct PolicyScopeRegistry {
    records: BTreeMap<PolicyScopeId, ScopeRecord>,
    next_permit: u64,
    released: BTreeSet<AdmissionPermitId>,
}
impl PolicyScopeRegistry {
    pub fn ensure_scope(&mut self, scope: PolicyScopeId) {
        self.records.entry(scope).or_insert_with(|| ScopeRecord {
            policy: ScopePolicy::default(),
            pending: BTreeMap::new(),
            admissions: Vec::new(),
            permits: BTreeSet::new(),
        });
    }
    pub(crate) fn get(&self, scope: &PolicyScopeId) -> Option<&ScopeRecord> {
        self.records.get(scope)
    }
    pub(crate) fn get_mut(&mut self, scope: &PolicyScopeId) -> Option<&mut ScopeRecord> {
        self.records.get_mut(scope)
    }
    pub fn configure(
        &mut self,
        scope: &PolicyScopeId,
        policy: ScopePolicy,
    ) -> Result<(), RegistryError> {
        self.records
            .get_mut(scope)
            .ok_or_else(|| RegistryError::UnknownScope(scope.clone()))?
            .policy = policy;
        Ok(())
    }
    pub fn policy(&self, scope: &PolicyScopeId) -> Result<&ScopePolicy, RegistryError> {
        Ok(&self
            .records
            .get(scope)
            .ok_or_else(|| RegistryError::UnknownScope(scope.clone()))?
            .policy)
    }
    pub fn pending_for_key(
        &self,
        scope: &PolicyScopeId,
        key: &Key,
    ) -> Result<Vec<RequestId>, RegistryError> {
        Ok(self
            .records
            .get(scope)
            .ok_or_else(|| RegistryError::UnknownScope(scope.clone()))?
            .pending
            .iter()
            .filter_map(|((_, request), pending_key)| (pending_key == key).then_some(*request))
            .collect())
    }
    pub fn add_pending(
        &mut self,
        scope: &PolicyScopeId,
        key: Key,
        request: RequestId,
        received_at: Duration,
    ) -> Result<(), RegistryError> {
        let r = self
            .records
            .get_mut(scope)
            .ok_or_else(|| RegistryError::UnknownScope(scope.clone()))?;
        r.pending.insert((received_at, request), key);
        Ok(())
    }
    pub fn remove_pending(
        &mut self,
        scope: &PolicyScopeId,
        request: RequestId,
    ) -> Result<bool, RegistryError> {
        let r = self
            .records
            .get_mut(scope)
            .ok_or_else(|| RegistryError::UnknownScope(scope.clone()))?;
        let before = r.pending.len();
        r.pending.retain(|(_, id), _| *id != request);
        Ok(before != r.pending.len())
    }
    /// The scope owns ordering; caller supplies current Key activity to keep truth with KeyRegistry.
    pub fn select_oldest_runnable<F>(
        &self,
        scope: &PolicyScopeId,
        mut key_is_active: F,
    ) -> Result<Option<RequestId>, RegistryError>
    where
        F: FnMut(&Key) -> bool,
    {
        let r = self
            .records
            .get(scope)
            .ok_or_else(|| RegistryError::UnknownScope(scope.clone()))?;
        Ok(r.pending
            .iter()
            .find_map(|((_, request), key)| (!key_is_active(key)).then_some(*request)))
    }
    pub fn try_reserve(
        &mut self,
        scope: &PolicyScopeId,
        now: Duration,
    ) -> Result<AdmissionReservation, RegistryError> {
        let decision = {
            let r = self
                .records
                .get(scope)
                .ok_or_else(|| RegistryError::UnknownScope(scope.clone()))?;
            evaluate_admission(
                &r.policy.admission,
                AdmissionState {
                    active_permits: r.permits.len(),
                    admissions: &r.admissions,
                },
                now,
            )
        };
        match decision {
            AdmissionDecision::Admit => {
                self.next_permit += 1;
                let permit = AdmissionPermitId(self.next_permit);
                let r = self.records.get_mut(scope).expect("scope was just checked");
                r.permits.insert(permit);
                r.admissions.push(now);
                Ok(AdmissionReservation::Reserved(permit))
            }
            AdmissionDecision::BlockOnCapacity => Ok(AdmissionReservation::BlockedOnCapacity),
            AdmissionDecision::BlockUntil(t) => Ok(AdmissionReservation::BlockedUntil(t)),
        }
    }
    pub fn release(
        &mut self,
        scope: &PolicyScopeId,
        permit: AdmissionPermitId,
    ) -> Result<(), RegistryError> {
        if self.released.contains(&permit) {
            return Err(RegistryError::PermitAlreadyReleased(permit));
        }
        let r = self
            .records
            .get_mut(scope)
            .ok_or_else(|| RegistryError::UnknownScope(scope.clone()))?;
        if !r.permits.remove(&permit) {
            return Err(RegistryError::UnknownPermit(permit));
        }
        self.released.insert(permit);
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionReservation {
    Reserved(AdmissionPermitId),
    BlockedOnCapacity,
    BlockedUntil(Duration),
}

#[cfg(test)]
mod tests {
    use super::*;
    fn definition() -> ExecutionDefinition {
        ExecutionDefinition {
            command: Command {
                executable: "true".into(),
                arguments: vec![],
                working_directory: None,
            },
            retry: RetryPolicy::default(),
            attempt_timeout: None,
            kill_grace: Duration::ZERO,
            unobserved_grace: None,
        }
    }
    #[test]
    fn stale_execution_never_clears_replacement() {
        let k = Key::from("k");
        let s = PolicyScopeId::for_key(&k, None);
        let mut keys = KeyRegistry::default();
        keys.upsert_definition(k.clone(), s, definition()).unwrap();
        keys.set_active(&k, ExecutionId(2)).unwrap();
        assert!(!keys.clear_active(&k, ExecutionId(1)).unwrap());
        assert_eq!(keys.active_execution(&k), Some(ExecutionId(2)));
    }
    #[test]
    fn scheduler_is_scope_wide_and_skips_active_key() {
        let a = Key::from("a");
        let b = Key::from("b");
        let s = PolicyScopeId::Group(GroupId::from("g"));
        let mut scopes = PolicyScopeRegistry::default();
        scopes.ensure_scope(s.clone());
        scopes
            .add_pending(&s, a.clone(), RequestId(1), Duration::from_secs(1))
            .unwrap();
        scopes
            .add_pending(&s, b.clone(), RequestId(2), Duration::from_secs(2))
            .unwrap();
        assert_eq!(
            scopes.select_oldest_runnable(&s, |k| k == &a).unwrap(),
            Some(RequestId(2))
        );
    }
    #[test]
    fn permit_is_consumed_and_released_once() {
        let k = Key::from("k");
        let s = PolicyScopeId::for_key(&k, None);
        let mut scopes = PolicyScopeRegistry::default();
        scopes.ensure_scope(s.clone());
        scopes
            .configure(
                &s,
                ScopePolicy {
                    contention: ContentionMode::Queue,
                    admission: AdmissionPolicy {
                        max_concurrency: Some(1),
                        rate_limit: None,
                    },
                },
            )
            .unwrap();
        let p = match scopes.try_reserve(&s, Duration::ZERO).unwrap() {
            AdmissionReservation::Reserved(p) => p,
            _ => panic!(),
        };
        assert_eq!(
            scopes.try_reserve(&s, Duration::ZERO).unwrap(),
            AdmissionReservation::BlockedOnCapacity
        );
        scopes.release(&s, p).unwrap();
        assert!(matches!(
            scopes.release(&s, p),
            Err(RegistryError::PermitAlreadyReleased(_))
        ));
    }
}
