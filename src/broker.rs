//! v2 Unix-socket adapter.  The application owns all lifecycle truth; this
//! module only serializes commands and performs process side effects.
use crate::{
    application::{Application, Effect, LifecycleEvent as AppEvent, StreamEvent, SubmitRequest},
    domain::{Command as DomainCommand, *},
    protocol::{self, *},
};
use anyhow::{Context, Result};
use std::{
    collections::HashMap, os::unix::process::CommandExt, path::PathBuf, sync::Arc, time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    net::{UnixListener, UnixStream},
    process::Command,
    sync::{Mutex, mpsc},
};

type Outbound = mpsc::UnboundedSender<BrokerMessage>;
type Subscribers = Arc<Mutex<HashMap<ExecutionId, HashMap<SubscriptionId, Outbound>>>>;
type Attempts = Arc<Mutex<HashMap<AttemptId, i32>>>;

pub async fn run(socket: PathBuf) -> Result<()> {
    let listener =
        UnixListener::bind(&socket).with_context(|| format!("listen on {}", socket.display()))?;
    let app = Arc::new(Mutex::new(Application::new()));
    let subscribers = Arc::new(Mutex::new(HashMap::new()));
    let attempts = Arc::new(Mutex::new(HashMap::new()));
    let mut next_subscription = 1_u64;
    loop {
        let (stream, _) = listener.accept().await.context("accept broker client")?;
        let id = SubscriptionId(next_subscription);
        next_subscription += 1;
        tokio::spawn(handle(
            stream,
            id,
            Arc::clone(&app),
            Arc::clone(&subscribers),
            Arc::clone(&attempts),
        ));
    }
}

async fn handle(
    stream: UnixStream,
    id: SubscriptionId,
    app: Arc<Mutex<Application>>,
    subscribers: Subscribers,
    attempts: Attempts,
) {
    let (mut reader, mut writer) = stream.into_split();
    let (out, mut rx) = mpsc::unbounded_channel();
    let writer_task = tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            if protocol::write_frame(&mut writer, &message).await.is_err() {
                break;
            }
        }
    });
    while let Ok(message) = protocol::read_frame::<_, ClientMessage>(&mut reader).await {
        match message {
            ClientMessage::Submit {
                cwd,
                argv,
                key,
                group,
                policies,
            } => {
                let (submit, scope_settings) = match make_submit(cwd, argv, key, group, policies) {
                    Ok(x) => x,
                    Err(e) => {
                        let _ = out.send(BrokerMessage::Error {
                            message: e.to_string(),
                        });
                        continue;
                    }
                };
                let (result, effects) = {
                    let mut a = app.lock().await;
                    let scope = PolicyScopeId::for_key(&submit.key, submit.group.clone());
                    a.configure_scope_patch(
                        scope,
                        scope_settings.contention,
                        scope_settings.max_concurrency,
                        scope_settings.rate_limit,
                    );
                    let r = a.submit(Duration::ZERO, submit);
                    (r, a.take_effects())
                };
                let _ = out.send(BrokerMessage::Submitted {
                    request_id: id_text(result.request_id),
                    execution_id: result.execution_id.map(id_text),
                });
                apply_effects(app.clone(), subscribers.clone(), attempts.clone(), effects);
            }
            ClientMessage::Subscribe { request_id, after } => match parse_id(&request_id) {
                Some(request) => {
                    let subscribed = {
                        let mut a = app.lock().await;
                        a.subscribe_with_replay(request, id, after.unwrap_or(0))
                    };
                    if let Some((execution, replay)) = subscribed {
                        subscribers
                            .lock()
                            .await
                            .entry(execution)
                            .or_default()
                            .insert(id, out.clone());
                        let _ = out.send(BrokerMessage::Subscribed {
                            subscription_id: id_text(id),
                            request_id,
                            execution_id: Some(id_text(execution)),
                        });
                        for event in replay {
                            let _ = out.send(wire_event(execution, event));
                        }
                    } else {
                        let _ = out.send(BrokerMessage::Error {
                            message: "unknown or not-yet-executing request".into(),
                        });
                    }
                }
                None => {
                    let _ = out.send(BrokerMessage::Error {
                        message: "invalid request id".into(),
                    });
                }
            },
            ClientMessage::CancelRequest { request_id } => {
                if let Some(request) = parse_id(&request_id) {
                    let (ok, effects) = {
                        let mut a = app.lock().await;
                        (a.cancel_request(Duration::ZERO, request), a.take_effects())
                    };
                    if ok {
                        let _ = out.send(BrokerMessage::Cancelled { request_id });
                        apply_effects(app.clone(), subscribers.clone(), attempts.clone(), effects);
                    }
                }
            }
            ClientMessage::Unsubscribe { subscription_id } => {
                if subscription_id == id_text(id) {
                    detach(&app, &subscribers, id).await;
                    let _ = out.send(BrokerMessage::Unsubscribed { subscription_id });
                }
            }
        }
    }
    detach(&app, &subscribers, id).await;
    writer_task.abort();
}

struct ScopeSettings {
    contention: Option<crate::domain::ContentionMode>,
    max_concurrency: Option<Option<usize>>,
    rate_limit: Option<Option<RateLimit>>,
}

fn make_submit(
    cwd: String,
    argv: Vec<String>,
    key: Option<String>,
    group: Option<String>,
    p: SubmitPolicies,
) -> Result<(SubmitRequest, ScopeSettings)> {
    if cwd.is_empty() || argv.first().is_none_or(String::is_empty) {
        anyhow::bail!("invalid submit command");
    }
    let key = Key(key.unwrap_or_else(|| format!("{cwd}\0{}", argv.join("\0"))));
    let scope_settings = ScopeSettings {
        contention: p.contention.map(|value| match value {
            protocol::ContentionMode::Reuse => crate::domain::ContentionMode::Reuse,
            protocol::ContentionMode::Queue => crate::domain::ContentionMode::Queue,
            protocol::ContentionMode::Latest => crate::domain::ContentionMode::Latest,
            protocol::ContentionMode::Drop => crate::domain::ContentionMode::Drop,
            protocol::ContentionMode::Replace => crate::domain::ContentionMode::Replace,
        }),
        max_concurrency: p.max_concurrency.map(Some),
        rate_limit: p.rate_limit.map(|rate| {
            Some(RateLimit {
                max_starts: rate.max_starts,
                per: Duration::from_millis(rate.per_ms),
            })
        }),
    };
    Ok((
        SubmitRequest {
            key,
            group: group.map(GroupId),
            definition: ExecutionDefinition {
                command: DomainCommand {
                    executable: argv[0].clone(),
                    arguments: argv[1..].to_vec(),
                    working_directory: Some(cwd),
                },
                retry: RetryPolicy {
                    limit: p.retry_limit,
                    retry_on: p.retry_on.map(|codes| codes.into_iter().collect()),
                    no_retry_on: p.no_retry_on.into_iter().collect(),
                    delay: Duration::from_millis(p.retry_delay_ms.unwrap_or(0)),
                    backoff: match p.retry_backoff {
                        protocol::RetryBackoff::Fixed => crate::domain::RetryBackoff::Fixed,
                        protocol::RetryBackoff::Exponential => {
                            crate::domain::RetryBackoff::Exponential
                        }
                    },
                    jitter_basis_points: p.retry_jitter_basis_points,
                    ..Default::default()
                },
                attempt_timeout: p.attempt_timeout_ms.map(Duration::from_millis),
                kill_grace: Duration::from_millis(p.kill_grace_ms.unwrap_or(1000)),
                unobserved_grace: p.unobserved_grace_ms.map(Duration::from_millis),
            },
        },
        scope_settings,
    ))
}
async fn detach(app: &Arc<Mutex<Application>>, subs: &Subscribers, id: SubscriptionId) {
    app.lock().await.disconnect(Duration::ZERO, id);
    for values in subs.lock().await.values_mut() {
        values.remove(&id);
    }
}
fn apply_effects(
    app: Arc<Mutex<Application>>,
    subs: Subscribers,
    attempts: Attempts,
    effects: Vec<Effect>,
) {
    for effect in effects {
        match effect {
            Effect::StartAttempt {
                execution_id,
                attempt_id,
                definition,
            } => {
                tokio::spawn(run_attempt(
                    app.clone(),
                    subs.clone(),
                    attempts.clone(),
                    execution_id,
                    attempt_id,
                    definition,
                ));
            }
            Effect::ScheduleRetry {
                execution_id,
                generation,
                at,
            } => {
                let app = app.clone();
                let subs = subs.clone();
                let attempts = attempts.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(at).await;
                    let effects = {
                        let mut a = app.lock().await;
                        let _ = a.retry_due(Duration::ZERO, execution_id, generation);
                        a.take_effects()
                    };
                    apply_effects(app, subs, attempts.clone(), effects);
                });
            }
            Effect::ScheduleUnobservedGrace {
                execution_id,
                generation,
                at,
            } => {
                let app = app.clone();
                let subs = subs.clone();
                let attempts = attempts.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(at).await;
                    let effects = {
                        let mut a = app.lock().await;
                        let _ = a.unobserved_grace_expired(execution_id, generation);
                        a.take_effects()
                    };
                    apply_effects(app, subs, attempts.clone(), effects);
                });
            }
            Effect::ScheduleAttemptTimeout {
                execution_id,
                attempt_id,
                generation,
                at,
            } => {
                let app = app.clone();
                let subs = subs.clone();
                let attempts = attempts.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(at).await;
                    let effects = {
                        let mut a = app.lock().await;
                        let _ = a.attempt_timeout_expired(execution_id, attempt_id, generation);
                        a.take_effects()
                    };
                    apply_effects(app, subs, attempts, effects);
                });
            }
            Effect::CancelAttempt {
                attempt_id,
                kill_grace,
                ..
            } => {
                let attempts = attempts.clone();
                tokio::spawn(async move {
                    if let Some(group) = attempts.lock().await.get(&attempt_id).copied() {
                        terminate_process_group(group, kill_grace).await;
                    }
                });
            }
        }
    }
}
async fn run_attempt(
    app: Arc<Mutex<Application>>,
    subs: Subscribers,
    attempts: Attempts,
    execution: ExecutionId,
    attempt: AttemptId,
    definition: ExecutionDefinition,
) {
    let mut command = Command::new(&definition.command.executable);
    command
        .args(&definition.command.arguments)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
    }
    if let Some(cwd) = definition.command.working_directory {
        command.current_dir(cwd);
    }
    let mut child = match command.spawn() {
        Ok(c) => c,
        Err(_) => {
            complete(
                app,
                subs,
                attempts,
                execution,
                attempt,
                Outcome::Failed {
                    exit_code: None,
                    signal: None,
                },
            )
            .await;
            return;
        }
    };
    let Some(pid) = child.id() else {
        complete(
            app,
            subs,
            attempts,
            execution,
            attempt,
            Outcome::Failed {
                exit_code: None,
                signal: None,
            },
        )
        .await;
        return;
    };
    attempts.lock().await.insert(attempt, pid as i32);
    let mut stdout = child.stdout.take();
    let mut stderr = child.stderr.take();
    let a = app.clone();
    let s = subs.clone();
    tokio::spawn(async move {
        if let Some(mut r) = stdout.take() {
            let mut b = vec![0; 8192];
            while let Ok(n) = r.read(&mut b).await {
                if n == 0 {
                    break;
                }
                publish_output(
                    a.clone(),
                    s.clone(),
                    execution,
                    attempt,
                    OutputStream::Stdout,
                    b[..n].to_vec(),
                )
                .await;
            }
        }
    });
    let a = app.clone();
    let s = subs.clone();
    tokio::spawn(async move {
        if let Some(mut r) = stderr.take() {
            let mut b = vec![0; 8192];
            while let Ok(n) = r.read(&mut b).await {
                if n == 0 {
                    break;
                }
                publish_output(
                    a.clone(),
                    s.clone(),
                    execution,
                    attempt,
                    OutputStream::Stderr,
                    b[..n].to_vec(),
                )
                .await;
            }
        }
    });
    let outcome = match child.wait().await {
        Ok(status) if status.success() => Outcome::Succeeded,
        Ok(status) => Outcome::Failed {
            exit_code: status.code(),
            signal: None,
        },
        Err(_) => Outcome::Failed {
            exit_code: None,
            signal: None,
        },
    };
    attempts.lock().await.remove(&attempt);
    complete(app, subs, attempts, execution, attempt, outcome).await;
}
async fn publish_output(
    app: Arc<Mutex<Application>>,
    subs: Subscribers,
    e: ExecutionId,
    a: AttemptId,
    stream: OutputStream,
    data: Vec<u8>,
) {
    let events = {
        let mut x = app.lock().await;
        if !x.record_output(e, a, stream, data) {
            return;
        }
        x.events_since(e, 0)
    };
    broadcast(&subs, e, events.last().cloned().unwrap()).await;
}
async fn complete(
    app: Arc<Mutex<Application>>,
    subs: Subscribers,
    attempts: Attempts,
    e: ExecutionId,
    a: AttemptId,
    outcome: Outcome,
) {
    let events = {
        let mut x = app.lock().await;
        let cursor = x
            .events_since(e, 0)
            .last()
            .map_or(0, |event| event.sequence);
        if !x.complete_attempt(Duration::ZERO, e, a, outcome) {
            return;
        }
        let effects = x.take_effects();
        let events = x.events_since(e, cursor);
        drop(x);
        apply_effects(app.clone(), subs.clone(), attempts, effects);
        events
    };
    for event in events {
        broadcast(&subs, e, event).await;
    }
}
async fn broadcast(subs: &Subscribers, e: ExecutionId, event: StreamEvent) {
    for out in subs
        .lock()
        .await
        .get(&e)
        .into_iter()
        .flat_map(|x| x.values())
    {
        let _ = out.send(wire_event(e, event.clone()));
    }
}
fn wire_event(execution: ExecutionId, event: StreamEvent) -> BrokerMessage {
    let correlated_request = event.request_id.map(id_text).unwrap_or_default();
    let (request, kind) = match event.event {
        AppEvent::Output {
            attempt_id,
            stream,
            data,
        } => (
            correlated_request.clone(),
            LifecycleEventKind::Output {
                attempt_id: id_text(attempt_id),
                stream,
                data_base64: encode_output(&data),
            },
        ),
        AppEvent::Execution {
            execution_id,
            state,
            outcome,
        } => (
            correlated_request.clone(),
            match state {
                ExecutionState::Running => LifecycleEventKind::ExecutionCreated,
                ExecutionState::Cancelling => LifecycleEventKind::ExecutionCancelling {
                    reason: "cancelled".into(),
                },
                _ => LifecycleEventKind::ExecutionCompleted {
                    outcome: outcome
                        .map(wire_outcome)
                        .unwrap_or(WireOutcome::Cancelled { reason: None }),
                },
            },
        ),
        AppEvent::Attempt {
            attempt_id,
            outcome,
            ..
        } => (
            correlated_request.clone(),
            LifecycleEventKind::AttemptCompleted {
                attempt_id: id_text(attempt_id),
                outcome: wire_outcome(outcome),
            },
        ),
        AppEvent::Request {
            request_id,
            state,
            outcome,
        } => {
            let kind = match (state, outcome) {
                (
                    RequestState::Succeeded
                    | RequestState::Failed
                    | RequestState::TimedOut
                    | RequestState::Cancelled,
                    Some(outcome),
                ) => LifecycleEventKind::RequestCompleted {
                    outcome: wire_outcome(outcome),
                },
                (RequestState::Attached, _) => LifecycleEventKind::RequestAttached,
                (RequestState::Pending, _) => LifecycleEventKind::RequestPending,
                (RequestState::Assigned, _) => LifecycleEventKind::RequestAssigned,
                (RequestState::Dropped, Some(Outcome::Dropped { reason })) => {
                    LifecycleEventKind::RequestDropped { reason }
                }
                (RequestState::Superseded, Some(Outcome::Superseded { by })) => {
                    LifecycleEventKind::RequestSuperseded {
                        by_request_id: id_text(by),
                    }
                }
                (RequestState::Rejected, Some(Outcome::Rejected { reason })) => {
                    LifecycleEventKind::RequestRejected { reason }
                }
                _ => LifecycleEventKind::RequestReceived,
            };
            (id_text(request_id), kind)
        }
    };
    BrokerMessage::Event {
        event: crate::protocol::LifecycleEvent {
            sequence: event.sequence,
            request_id: request,
            execution_id: Some(id_text(execution)),
            kind,
        },
    }
}
fn wire_outcome(o: Outcome) -> WireOutcome {
    match o {
        Outcome::Succeeded => WireOutcome::Succeeded,
        Outcome::Failed { exit_code, signal } => WireOutcome::Failed {
            code: exit_code,
            signal,
        },
        Outcome::TimedOut => WireOutcome::TimedOut,
        Outcome::Cancelled => WireOutcome::Cancelled { reason: None },
        Outcome::Dropped { reason } => WireOutcome::Dropped { reason },
        Outcome::Superseded { by } => WireOutcome::Superseded {
            by_request_id: id_text(by),
        },
        Outcome::Rejected { reason } => WireOutcome::Rejected { reason },
    }
}
trait IdText {
    fn text(self) -> String;
}
macro_rules! id_text { ($($type:ty),* $(,)?) => { $(impl IdText for $type { fn text(self) -> String { self.0.to_string() } })* }; }
id_text!(RequestId, ExecutionId, AttemptId, SubscriptionId);
fn id_text<T: IdText>(id: T) -> String {
    id.text()
}
fn parse_id(text: &str) -> Option<RequestId> {
    text.parse().ok().map(RequestId)
}

fn signal_process_group(group: i32, signal: i32) {
    unsafe {
        libc::kill(-group, signal);
    }
}
async fn terminate_process_group(group: i32, kill_grace: Duration) {
    signal_process_group(group, libc::SIGTERM);
    tokio::time::sleep(kill_grace).await;
    signal_process_group(group, libc::SIGKILL);
}
