//! Deterministic domain values and policy decisions for foxrun v2.
//!
//! This module deliberately has no knowledge of Tokio, sockets, or processes.

use std::collections::BTreeSet;
use std::time::Duration;

macro_rules! opaque_id {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(pub u64);

        impl From<u64> for $name {
            fn from(value: u64) -> Self {
                Self(value)
            }
        }
    };
}

opaque_id!(RequestId);
opaque_id!(ExecutionId);
opaque_id!(AttemptId);
opaque_id!(SubscriptionId);
opaque_id!(DefinitionVersion);
opaque_id!(AdmissionPermitId);

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Key(pub String);

impl From<&str> for Key {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}
impl From<String> for Key {
    fn from(value: String) -> Self {
        Self(value)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GroupId(pub String);

impl From<&str> for GroupId {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

/// A scope is either the private scope naturally owned by a Key or a named group.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PolicyScopeId {
    Key(Key),
    Group(GroupId),
}

impl PolicyScopeId {
    pub fn for_key(key: &Key, group: Option<GroupId>) -> Self {
        group
            .map(Self::Group)
            .unwrap_or_else(|| Self::Key(key.clone()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Command {
    pub executable: String,
    pub arguments: Vec<String>,
    pub working_directory: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionDefinition {
    pub command: Command,
    pub retry: RetryPolicy,
    pub attempt_timeout: Option<Duration>,
    pub kill_grace: Duration,
    pub unobserved_grace: Option<Duration>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Request {
    pub id: RequestId,
    pub key: Key,
    pub scope: PolicyScopeId,
    pub definition: ExecutionDefinition,
    pub received_at: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestState {
    Received,
    Attached,
    Pending,
    Assigned,
    Succeeded,
    Failed,
    TimedOut,
    Cancelled,
    Dropped,
    Superseded,
    Rejected,
}

impl RequestState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded
                | Self::Failed
                | Self::TimedOut
                | Self::Cancelled
                | Self::Dropped
                | Self::Superseded
                | Self::Rejected
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionState {
    Created,
    Running,
    RetryWaiting,
    Cancelling,
    Succeeded,
    Failed,
    TimedOut,
    Cancelled,
}
impl ExecutionState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::TimedOut | Self::Cancelled
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptState {
    Starting,
    Running,
    Terminating,
    Killing,
    Succeeded,
    Failed,
    TimedOut,
    Cancelled,
    SpawnFailed,
}
impl AttemptState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::TimedOut | Self::Cancelled | Self::SpawnFailed
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Outcome {
    Succeeded,
    Failed {
        exit_code: Option<i32>,
        signal: Option<i32>,
    },
    TimedOut,
    Cancelled,
    Dropped {
        reason: String,
    },
    Superseded {
        by: RequestId,
    },
    Rejected {
        reason: String,
    },
}

impl Outcome {
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Succeeded)
    }
    pub fn request_state(&self) -> RequestState {
        match self {
            Self::Succeeded => RequestState::Succeeded,
            Self::Failed { .. } => RequestState::Failed,
            Self::TimedOut => RequestState::TimedOut,
            Self::Cancelled => RequestState::Cancelled,
            Self::Dropped { .. } => RequestState::Dropped,
            Self::Superseded { .. } => RequestState::Superseded,
            Self::Rejected { .. } => RequestState::Rejected,
        }
    }
    pub fn execution_state(&self) -> Option<ExecutionState> {
        match self {
            Self::Succeeded => Some(ExecutionState::Succeeded),
            Self::Failed { .. } => Some(ExecutionState::Failed),
            Self::TimedOut => Some(ExecutionState::TimedOut),
            Self::Cancelled => Some(ExecutionState::Cancelled),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Execution {
    pub id: ExecutionId,
    pub key: Key,
    pub scope: PolicyScopeId,
    pub definition_version: DefinitionVersion,
    pub definition: ExecutionDefinition,
    pub permit: AdmissionPermitId,
    pub state: ExecutionState,
    pub attempts: Vec<AttemptId>,
    pub outcome: Option<Outcome>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Attempt {
    pub id: AttemptId,
    pub execution_id: ExecutionId,
    pub state: AttemptState,
    pub outcome: Option<Outcome>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContentionMode {
    Reuse,
    Queue,
    Latest,
    Drop,
    Replace,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScopePolicy {
    pub contention: ContentionMode,
    pub admission: AdmissionPolicy,
}
impl Default for ScopePolicy {
    fn default() -> Self {
        Self {
            contention: ContentionMode::Reuse,
            admission: AdmissionPolicy::default(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContentionContext<'a> {
    pub mode: ContentionMode,
    pub active_execution: Option<ExecutionId>,
    pub pending_for_key: &'a [RequestId],
    /// True only when admission and queue ordering say a fresh execution could begin now.
    pub can_start_fresh: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContentionDecision {
    Attach(ExecutionId),
    Pend,
    SupersedePendingAndPend(Vec<RequestId>),
    Drop,
    Replace(ExecutionId),
}

pub fn decide_contention(context: ContentionContext<'_>) -> ContentionDecision {
    match context.mode {
        ContentionMode::Reuse => context
            .active_execution
            .map(ContentionDecision::Attach)
            .unwrap_or(ContentionDecision::Pend),
        ContentionMode::Queue => ContentionDecision::Pend,
        ContentionMode::Latest => {
            ContentionDecision::SupersedePendingAndPend(context.pending_for_key.to_vec())
        }
        ContentionMode::Drop => {
            if context.active_execution.is_none()
                && context.pending_for_key.is_empty()
                && context.can_start_fresh
            {
                ContentionDecision::Pend
            } else {
                ContentionDecision::Drop
            }
        }
        ContentionMode::Replace => context
            .active_execution
            .map(ContentionDecision::Replace)
            .unwrap_or_else(|| {
                if context.pending_for_key.is_empty() {
                    ContentionDecision::Pend
                } else {
                    ContentionDecision::SupersedePendingAndPend(context.pending_for_key.to_vec())
                }
            }),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RateLimit {
    pub max_starts: usize,
    pub per: Duration,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AdmissionPolicy {
    pub max_concurrency: Option<usize>,
    pub rate_limit: Option<RateLimit>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionState<'a> {
    pub active_permits: usize,
    pub admissions: &'a [Duration],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionDecision {
    Admit,
    BlockOnCapacity,
    BlockUntil(Duration),
}

pub fn evaluate_admission(
    policy: &AdmissionPolicy,
    state: AdmissionState<'_>,
    now: Duration,
) -> AdmissionDecision {
    if policy
        .max_concurrency
        .is_some_and(|max| state.active_permits >= max)
    {
        return AdmissionDecision::BlockOnCapacity;
    }
    if let Some(limit) = &policy.rate_limit {
        if limit.max_starts == 0 {
            return AdmissionDecision::BlockUntil(Duration::MAX);
        }
        let earliest = now.checked_sub(limit.per).unwrap_or(Duration::ZERO);
        let relevant: Vec<_> = state
            .admissions
            .iter()
            .copied()
            .filter(|start| *start > earliest)
            .collect();
        if relevant.len() >= limit.max_starts {
            let first = relevant[relevant.len() - limit.max_starts];
            return AdmissionDecision::BlockUntil(first.saturating_add(limit.per));
        }
    }
    AdmissionDecision::Admit
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryBackoff {
    Fixed,
    Exponential,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    /// Number of retries after the initial attempt.
    pub limit: u32,
    pub retry_on: Option<BTreeSet<i32>>,
    pub no_retry_on: BTreeSet<i32>,
    pub delay: Duration,
    pub backoff: RetryBackoff,
    /// A symmetric fraction in basis points; randomness is supplied by the caller.
    pub jitter_basis_points: u16,
}
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            limit: 0,
            retry_on: None,
            no_retry_on: BTreeSet::new(),
            delay: Duration::ZERO,
            backoff: RetryBackoff::Fixed,
            jitter_basis_points: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryDecision {
    Complete,
    RetryAfter(Duration),
}

/// `attempts_completed` includes the just-completed attempt. `jitter` is in
/// [-10_000, 10_000] basis points, allowing deterministic callers to pass zero.
pub fn decide_retry(
    policy: &RetryPolicy,
    attempts_completed: u32,
    outcome: &Outcome,
    jitter: i16,
) -> RetryDecision {
    if outcome.is_success()
        || matches!(
            outcome,
            Outcome::Cancelled
                | Outcome::Dropped { .. }
                | Outcome::Superseded { .. }
                | Outcome::Rejected { .. }
        )
        || attempts_completed == 0
        || attempts_completed >= policy.limit.saturating_add(1)
    {
        return RetryDecision::Complete;
    }
    let code = match outcome {
        Outcome::Failed { exit_code, .. } => *exit_code,
        _ => None,
    };
    if code.is_some_and(|code| policy.no_retry_on.contains(&code))
        || policy
            .retry_on
            .as_ref()
            .is_some_and(|allowed| !code.is_some_and(|code| allowed.contains(&code)))
    {
        return RetryDecision::Complete;
    }
    let exponent = attempts_completed.saturating_sub(1).min(63);
    let multiplier = match policy.backoff {
        RetryBackoff::Fixed => 1,
        RetryBackoff::Exponential => 1u32.checked_shl(exponent).unwrap_or(u32::MAX),
    };
    let base = policy.delay.saturating_mul(multiplier);
    let bounded = jitter.clamp(
        -(policy.jitter_basis_points as i16),
        policy.jitter_basis_points as i16,
    ) as i64;
    let nanos = base.as_nanos();
    let adjusted = if bounded >= 0 {
        nanos.saturating_add(nanos.saturating_mul(bounded as u128) / 10_000)
    } else {
        nanos.saturating_sub(nanos.saturating_mul((-bounded) as u128) / 10_000)
    };
    RetryDecision::RetryAfter(Duration::from_nanos(adjusted.min(u64::MAX as u128) as u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reuse_attaches_but_latest_supersedes_only_pending() {
        let pending = [RequestId(2)];
        assert_eq!(
            decide_contention(ContentionContext {
                mode: ContentionMode::Reuse,
                active_execution: Some(ExecutionId(3)),
                pending_for_key: &pending,
                can_start_fresh: false
            }),
            ContentionDecision::Attach(ExecutionId(3))
        );
        assert_eq!(
            decide_contention(ContentionContext {
                mode: ContentionMode::Latest,
                active_execution: Some(ExecutionId(3)),
                pending_for_key: &pending,
                can_start_fresh: false
            }),
            ContentionDecision::SupersedePendingAndPend(vec![RequestId(2)])
        );
    }
    #[test]
    fn admission_rate_waits_for_oldest_relevant_start() {
        let p = AdmissionPolicy {
            max_concurrency: Some(2),
            rate_limit: Some(RateLimit {
                max_starts: 2,
                per: Duration::from_secs(10),
            }),
        };
        assert_eq!(
            evaluate_admission(
                &p,
                AdmissionState {
                    active_permits: 0,
                    admissions: &[Duration::from_secs(1), Duration::from_secs(4)]
                },
                Duration::from_secs(5)
            ),
            AdmissionDecision::BlockUntil(Duration::from_secs(11))
        );
    }
    #[test]
    fn retry_limit_means_additional_attempts() {
        let p = RetryPolicy {
            limit: 2,
            delay: Duration::from_secs(1),
            backoff: RetryBackoff::Exponential,
            ..Default::default()
        };
        assert_eq!(
            decide_retry(
                &p,
                1,
                &Outcome::Failed {
                    exit_code: Some(1),
                    signal: None
                },
                0
            ),
            RetryDecision::RetryAfter(Duration::from_secs(1))
        );
        assert_eq!(
            decide_retry(
                &p,
                3,
                &Outcome::Failed {
                    exit_code: Some(1),
                    signal: None
                },
                0
            ),
            RetryDecision::Complete
        );
    }
}
