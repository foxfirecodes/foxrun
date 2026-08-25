//! The local process broker.
//!
//! This module deliberately keeps process ownership in one actor.  Socket
//! tasks only turn connections into leases and forward the actor's messages.

use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};

use crate::protocol::{self, AcquireRequest, BrokerMessage, ClientMessage, OutputStream};

const HISTORY_LINES: usize = 1_000;
const OUTPUT_CHUNK: usize = 8 * 1024;
// One output frame may be in flight. Keep a second slot for the terminal exit
// frame so it remains ordered after the final output frame.
const CLIENT_WRITER_QUEUE: usize = 2;
const CLIENT_BACKLOG_BYTES: usize = protocol::MAX_FRAME_SIZE;

type ClientId = u64;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct ProcessKey(String);

#[derive(Serialize)]
struct Identity<'a> {
    cwd: &'a str,
    argv: &'a [String],
}

impl ProcessKey {
    fn new(request: &AcquireRequest) -> Result<Self> {
        // Keep the serialized value as well as relying on HashMap's hash. This
        // meets the structured-key requirement and cannot confuse two values
        // merely because their argv would look the same when space-joined.
        let json = serde_json::to_string(&Identity {
            cwd: &request.cwd,
            argv: &request.argv,
        })?;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        json.hash(&mut hasher);
        Ok(Self(format!("{:016x}:{json}", hasher.finish())))
    }
}

#[derive(Clone)]
struct Client {
    id: ClientId,
    messages: mpsc::Sender<BrokerMessage>,
    close: mpsc::UnboundedSender<()>,
}

#[derive(Clone)]
struct OutputChunk {
    message: BrokerMessage,
    wire_bytes: usize,
}

impl OutputChunk {
    fn new(stream: OutputStream, data: &[u8]) -> Self {
        let message = BrokerMessage::output(stream, data);
        let wire_bytes = serde_json::to_vec(&message)
            .expect("output messages always serialize")
            .len()
            + std::mem::size_of::<u32>();
        Self {
            message,
            wire_bytes,
        }
    }
}

struct ByteQueue {
    messages: VecDeque<OutputChunk>,
    wire_bytes: usize,
}

impl ByteQueue {
    fn new() -> Self {
        Self {
            messages: VecDeque::new(),
            wire_bytes: 0,
        }
    }

    fn push(&mut self, message: OutputChunk, in_flight_bytes: usize) -> bool {
        let Some(total) = self
            .wire_bytes
            .checked_add(in_flight_bytes)
            .and_then(|bytes| bytes.checked_add(message.wire_bytes))
        else {
            return false;
        };
        if total > CLIENT_BACKLOG_BYTES {
            return false;
        }
        self.wire_bytes += message.wire_bytes;
        self.messages.push_back(message);
        true
    }

    fn pop(&mut self) -> Option<OutputChunk> {
        let message = self.messages.pop_front()?;
        self.wire_bytes -= message.wire_bytes;
        Some(message)
    }
}

struct Replay {
    lines: VecDeque<HistoryLine>,
    offset: usize,
}

impl Replay {
    fn new(lines: VecDeque<HistoryLine>) -> Self {
        Self { lines, offset: 0 }
    }

    fn next(&mut self) -> Option<OutputChunk> {
        let line = self.lines.front()?;
        let end = (self.offset + OUTPUT_CHUNK).min(line.data.len());
        let chunk = OutputChunk::new(line.stream, &line.data[self.offset..end]);
        if end == line.data.len() {
            self.lines.pop_front();
            self.offset = 0;
        } else {
            self.offset = end;
        }
        Some(chunk)
    }
}

enum DeliveryState {
    Replaying { replay: Replay, pending: ByteQueue },
    Live { pending: ByteQueue },
}

struct Delivery {
    client: Client,
    state: DeliveryState,
    writer_ready: bool,
    in_flight_bytes: usize,
}

impl Delivery {
    fn replaying(client: Client, lines: VecDeque<HistoryLine>) -> Self {
        Self {
            client,
            state: DeliveryState::Replaying {
                replay: Replay::new(lines),
                pending: ByteQueue::new(),
            },
            writer_ready: false,
            in_flight_bytes: 0,
        }
    }

    fn live(client: Client) -> Self {
        Self {
            client,
            state: DeliveryState::Live {
                pending: ByteQueue::new(),
            },
            writer_ready: false,
            in_flight_bytes: 0,
        }
    }

    fn enqueue(&mut self, message: OutputChunk) -> bool {
        let pending = match &mut self.state {
            DeliveryState::Replaying { pending, .. } | DeliveryState::Live { pending } => pending,
        };
        pending.push(message, self.in_flight_bytes)
    }

    fn writer_ready(&mut self) -> bool {
        self.writer_ready = true;
        self.in_flight_bytes = 0;
        self.deliver_next()
    }

    fn deliver_next(&mut self) -> bool {
        if !self.writer_ready {
            return true;
        }
        let message = loop {
            match &mut self.state {
                DeliveryState::Replaying { replay, .. } => {
                    if let Some(message) = replay.next() {
                        break Some(message);
                    }
                    let DeliveryState::Replaying { pending, .. } = std::mem::replace(
                        &mut self.state,
                        DeliveryState::Live {
                            pending: ByteQueue::new(),
                        },
                    ) else {
                        unreachable!("state was checked above");
                    };
                    self.state = DeliveryState::Live { pending };
                }
                DeliveryState::Live { pending } => break pending.pop(),
            }
        };
        let Some(message) = message else {
            return true;
        };
        if !send(&self.client, message.message.clone()) {
            return false;
        }
        self.writer_ready = false;
        self.in_flight_bytes = message.wire_bytes;
        true
    }
}

struct ProcessRecord {
    timeout: Duration,
    process_group: i32,
    clients: HashMap<ClientId, Delivery>,
    history: VecDeque<HistoryLine>,
    partial_stdout: Vec<u8>,
    partial_stderr: Vec<u8>,
    idle_generation: u64,
}

#[derive(Clone)]
struct HistoryLine {
    stream: OutputStream,
    data: Vec<u8>,
}

enum Event {
    Acquire {
        request: AcquireRequest,
        client: Client,
        response: oneshot::Sender<Result<(), String>>,
    },
    Disconnect {
        key: ProcessKey,
        client_id: ClientId,
    },
    Output {
        key: ProcessKey,
        stream: OutputStream,
        data: Vec<u8>,
    },
    Exited {
        key: ProcessKey,
        code: Option<i32>,
        signal: Option<i32>,
    },
    IdleExpired {
        key: ProcessKey,
        generation: u64,
    },
    WriterReady {
        client_id: ClientId,
    },
    ConnectionClosed,
}

/// Serve a broker until it has no live commands or client leases.
pub async fn run(socket: PathBuf) -> Result<()> {
    let listener =
        UnixListener::bind(&socket).with_context(|| format!("listen on {}", socket.display()))?;
    let (events, mut receiver) = mpsc::channel(256);
    let events = Arc::new(events);
    let mut records = HashMap::<ProcessKey, ProcessRecord>::new();
    let mut client_keys = HashMap::<ClientId, ProcessKey>::new();
    let mut next_client = 1_u64;
    let mut connections = 0_usize;
    // The launching client connects only after this process has bound its
    // socket. Do not treat that short window as an idle broker.
    let mut accepted_connection = false;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    accepted_connection = true;
                    let id = next_client;
                    next_client = next_client.wrapping_add(1);
                    connections += 1;
                    let connection_events = Arc::clone(&events);
                    tokio::spawn(async move {
                        handle_connection(stream, id, Arc::clone(&connection_events)).await;
                        let _ = connection_events.send(Event::ConnectionClosed).await;
                    });
                }
                Err(error) => return Err(error).context("accept broker client"),
            },
            event = receiver.recv() => match event {
                Some(Event::ConnectionClosed) => connections = connections.saturating_sub(1),
                Some(event) => handle_event(event, &mut records, &mut client_keys, &events).await,
                None => break,
            },
            _ = interrupt.recv() => break,
            _ = terminate.recv() => break,
        }

        if accepted_connection && records.is_empty() && connections == 0 {
            // There cannot be a lease once its record has gone. The next
            // invocation starts a new broker if it races this orderly exit.
            break;
        }
    }

    terminate_all(records.values().map(|record| record.process_group)).await;
    let _ = std::fs::remove_file(&socket);
    Ok(())
}

async fn handle_connection(
    stream: UnixStream,
    client_id: ClientId,
    events: Arc<mpsc::Sender<Event>>,
) {
    let (mut reader, mut writer) = stream.into_split();
    let (messages, mut outbound) = mpsc::channel(CLIENT_WRITER_QUEUE);
    let (close, mut close_rx) = mpsc::unbounded_channel();
    let (writer_closed, mut writer_closed_rx) = oneshot::channel();
    let writer_events = Arc::clone(&events);
    tokio::spawn(async move {
        while let Some(message) = outbound.recv().await {
            if protocol::write_message(&mut writer, &message)
                .await
                .is_err()
            {
                break;
            }
            if matches!(
                message,
                BrokerMessage::Exit { .. } | BrokerMessage::Error { .. }
            ) {
                break;
            }
            if writer_events
                .send(Event::WriterReady { client_id })
                .await
                .is_err()
            {
                break;
            }
        }
        let _ = writer_closed.send(());
    });

    let request = match protocol::read_message::<_, ClientMessage>(&mut reader).await {
        Ok(message) => match message.validate() {
            Ok(request) => request,
            Err(error) => {
                let _ = messages
                    .send(BrokerMessage::Error {
                        message: error.to_string(),
                    })
                    .await;
                let _ = (&mut writer_closed_rx).await;
                return;
            }
        },
        Err(error) => {
            let _ = messages
                .send(BrokerMessage::Error {
                    message: error.to_string(),
                })
                .await;
            let _ = (&mut writer_closed_rx).await;
            return;
        }
    };
    let valid_cwd = std::fs::canonicalize(&request.cwd)
        .map(|path| path.is_dir() && path == Path::new(&request.cwd))
        .unwrap_or(false);
    if !Path::new(&request.cwd).is_absolute() || !valid_cwd {
        let _ = messages
            .send(BrokerMessage::Error {
                message: "cwd must be an existing canonical absolute directory".into(),
            })
            .await;
        let _ = (&mut writer_closed_rx).await;
        return;
    }
    let key = match ProcessKey::new(&request) {
        Ok(key) => key,
        Err(error) => {
            let _ = messages
                .send(BrokerMessage::Error {
                    message: error.to_string(),
                })
                .await;
            let _ = (&mut writer_closed_rx).await;
            return;
        }
    };
    let (response_tx, response_rx) = oneshot::channel();
    if events
        .send(Event::Acquire {
            request,
            client: Client {
                id: client_id,
                messages: messages.clone(),
                close,
            },
            response: response_tx,
        })
        .await
        .is_err()
    {
        return;
    }
    match response_rx.await {
        Ok(Ok(())) => {}
        Ok(Err(message)) => {
            let _ = messages.send(BrokerMessage::Error { message }).await;
            let _ = (&mut writer_closed_rx).await;
            return;
        }
        Err(_) => return,
    }

    // There is no further client message in this protocol. Reading until EOF
    // both detects a closed lease and rejects accidental extra frames.
    tokio::select! {
        read = protocol::read_message::<_, ClientMessage>(&mut reader) => {
            if read.is_ok() {
                let _ = messages.send(BrokerMessage::Error {
                    message: "a connection may send only one acquire request".into(),
                }).await;
                let _ = (&mut writer_closed_rx).await;
            }
        }
        _ = &mut writer_closed_rx => {}
        _ = close_rx.recv() => {}
    }
    let _ = events.send(Event::Disconnect { key, client_id }).await;
}

async fn handle_event(
    event: Event,
    records: &mut HashMap<ProcessKey, ProcessRecord>,
    client_keys: &mut HashMap<ClientId, ProcessKey>,
    events: &mpsc::Sender<Event>,
) {
    match event {
        Event::Acquire {
            request,
            client,
            response,
        } => {
            let key = match ProcessKey::new(&request) {
                Ok(key) => key,
                Err(error) => {
                    let _ = response.send(Err(error.to_string()));
                    return;
                }
            };
            if let Some(record) = records.get_mut(&key) {
                record.timeout = Duration::from_millis(request.broker_timeout_ms);
                let tail = record
                    .history
                    .iter()
                    .skip(
                        record
                            .history
                            .len()
                            .saturating_sub(request.tail_lines.min(HISTORY_LINES)),
                    )
                    .cloned()
                    .collect::<VecDeque<_>>();
                if !send(&client, BrokerMessage::Attached { reused: true }) {
                    let _ = response.send(Err("client disconnected while attaching".into()));
                    return;
                }
                // Attached is in flight. Its writer acknowledgement starts
                // replay, so live output cannot overtake the requested tail.
                record.idle_generation = record.idle_generation.wrapping_add(1);
                client_keys.insert(client.id, key.clone());
                record
                    .clients
                    .insert(client.id, Delivery::replaying(client, tail));
                let _ = response.send(Ok(()));
                return;
            }

            match start_process(&key, &request, events.clone()).await {
                Ok(process_group) => {
                    if !send(&client, BrokerMessage::Attached { reused: false }) {
                        tokio::spawn(terminate_process_group(process_group));
                        let _ = response.send(Err("client disconnected while attaching".into()));
                        return;
                    }
                    let mut clients = HashMap::new();
                    client_keys.insert(client.id, key.clone());
                    clients.insert(client.id, Delivery::live(client));
                    records.insert(
                        key,
                        ProcessRecord {
                            timeout: Duration::from_millis(request.broker_timeout_ms),
                            process_group,
                            clients,
                            history: VecDeque::new(),
                            partial_stdout: Vec::new(),
                            partial_stderr: Vec::new(),
                            idle_generation: 0,
                        },
                    );
                    let _ = response.send(Ok(()));
                }
                Err(error) => {
                    let _ = response.send(Err(error.to_string()));
                }
            }
        }
        Event::Disconnect { key, client_id } => {
            // Writer errors don't carry the key. They are harmless here; the
            // reader's keyed disconnect follows when its socket is closed.
            let Some(record) = records.get_mut(&key) else {
                return;
            };
            client_keys.remove(&client_id);
            if record.clients.remove(&client_id).is_some() && record.clients.is_empty() {
                arm_idle(key, record, events.clone());
            }
        }
        Event::Output { key, stream, data } => {
            let Some(record) = records.get_mut(&key) else {
                return;
            };
            let had_clients = !record.clients.is_empty();
            append_history(record, stream, &data);
            let output = OutputChunk::new(stream, &data);
            let stale: Vec<_> = record
                .clients
                .iter_mut()
                .filter_map(|(&id, delivery)| {
                    (!delivery.enqueue(output.clone()) || !delivery.deliver_next()).then_some(id)
                })
                .collect();
            for id in stale {
                if let Some(delivery) = record.clients.remove(&id) {
                    client_keys.remove(&id);
                    close_client(&delivery.client);
                }
            }
            if had_clients && record.clients.is_empty() {
                arm_idle(key, record, events.clone());
            }
        }
        Event::Exited { key, code, signal } => {
            if let Some(record) = records.remove(&key) {
                for (id, delivery) in record.clients {
                    client_keys.remove(&id);
                    if !send(&delivery.client, BrokerMessage::Exit { code, signal }) {
                        close_client(&delivery.client);
                    }
                }
            }
        }
        Event::IdleExpired { key, generation } => {
            if let Some(record) = records.get(&key)
                && record.clients.is_empty()
                && record.idle_generation == generation
            {
                tokio::spawn(terminate_process_group(record.process_group));
            }
        }
        Event::WriterReady { client_id } => {
            let Some(key) = client_keys.get(&client_id).cloned() else {
                return;
            };
            let Some(record) = records.get_mut(&key) else {
                return;
            };
            let stale = record
                .clients
                .get_mut(&client_id)
                .is_some_and(|delivery| !delivery.writer_ready());
            if stale {
                if let Some(delivery) = record.clients.remove(&client_id) {
                    client_keys.remove(&client_id);
                    close_client(&delivery.client);
                }
                if record.clients.is_empty() {
                    arm_idle(key, record, events.clone());
                }
            }
        }
        Event::ConnectionClosed => unreachable!("handled by broker loop"),
    }
}

fn send(client: &Client, message: BrokerMessage) -> bool {
    client.messages.try_send(message).is_ok()
}

fn close_client(client: &Client) {
    let _ = client.close.send(());
}

fn arm_idle(key: ProcessKey, record: &mut ProcessRecord, events: mpsc::Sender<Event>) {
    record.idle_generation = record.idle_generation.wrapping_add(1);
    let generation = record.idle_generation;
    let timeout = record.timeout;
    tokio::spawn(async move {
        tokio::time::sleep(timeout).await;
        let _ = events.send(Event::IdleExpired { key, generation }).await;
    });
}

async fn start_process(
    key: &ProcessKey,
    request: &AcquireRequest,
    events: mpsc::Sender<Event>,
) -> Result<i32> {
    let mut command = Command::new(&request.argv[0]);
    command
        .args(&request.argv[1..])
        .current_dir(&request.cwd)
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
    let mut child = command.spawn().context("start command")?;
    let pid = child.id().context("started command has no pid")? as i32;
    let stdout = child
        .stdout
        .take()
        .context("command stdout was not captured")?;
    let stderr = child
        .stderr
        .take()
        .context("command stderr was not captured")?;
    let output_events = events.clone();
    let output_key = key.clone();
    let stdout_task = tokio::spawn(read_output(
        stdout,
        output_key.clone(),
        OutputStream::Stdout,
        output_events.clone(),
    ));
    let stderr_task = tokio::spawn(read_output(
        stderr,
        output_key.clone(),
        OutputStream::Stderr,
        output_events,
    ));
    tokio::spawn(async move {
        let status = child.wait().await;
        let _ = stdout_task.await;
        let _ = stderr_task.await;
        let (code, signal) = match status {
            Ok(status) => (status.code(), status.signal()),
            Err(_) => (None, None),
        };
        let _ = events
            .send(Event::Exited {
                key: output_key,
                code,
                signal,
            })
            .await;
    });
    Ok(pid)
}

async fn read_output<R: tokio::io::AsyncRead + Unpin>(
    mut reader: R,
    key: ProcessKey,
    stream: OutputStream,
    events: mpsc::Sender<Event>,
) {
    let mut buffer = vec![0; OUTPUT_CHUNK];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(length) => {
                if events
                    .send(Event::Output {
                        key: key.clone(),
                        stream,
                        data: buffer[..length].to_vec(),
                    })
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }
}

fn append_history(record: &mut ProcessRecord, stream: OutputStream, data: &[u8]) {
    let partial = match stream {
        OutputStream::Stdout => &mut record.partial_stdout,
        OutputStream::Stderr => &mut record.partial_stderr,
    };
    partial.extend_from_slice(data);
    while let Some(end) = partial.iter().position(|byte| *byte == b'\n') {
        let line: Vec<u8> = partial.drain(..=end).collect();
        record.history.push_back(HistoryLine { stream, data: line });
        if record.history.len() > HISTORY_LINES {
            record.history.pop_front();
        }
    }
}

fn signal_process_group(process_group: i32, signal: i32) {
    unsafe {
        libc::kill(-process_group, signal);
    }
}

async fn terminate_process_group(process_group: i32) {
    signal_process_group(process_group, libc::SIGTERM);
    tokio::time::sleep(Duration::from_secs(1)).await;
    signal_process_group(process_group, libc::SIGKILL);
}

async fn terminate_all(process_groups: impl Iterator<Item = i32>) {
    let process_groups: Vec<_> = process_groups.collect();
    for process_group in &process_groups {
        signal_process_group(*process_group, libc::SIGTERM);
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    for process_group in process_groups {
        signal_process_group(process_group, libc::SIGKILL);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn replays_more_chunks_than_the_writer_queue() {
        let (messages, mut outbound) = mpsc::channel(CLIENT_WRITER_QUEUE);
        let (close, _) = mpsc::unbounded_channel();
        let chunks = 65;
        let lines = (0..chunks)
            .map(|_| HistoryLine {
                stream: OutputStream::Stdout,
                data: vec![b'x'; OUTPUT_CHUNK],
            })
            .collect();
        let mut delivery = Delivery::replaying(
            Client {
                id: 1,
                messages,
                close,
            },
            lines,
        );

        for _ in 0..chunks {
            assert!(delivery.writer_ready());
            assert!(matches!(
                outbound.recv().await,
                Some(BrokerMessage::Output { .. })
            ));
        }
        assert!(delivery.writer_ready());
        assert!(outbound.try_recv().is_err());
    }

    #[tokio::test]
    async fn delivers_live_output_after_replay() {
        let (messages, mut outbound) = mpsc::channel(CLIENT_WRITER_QUEUE);
        let (close, _) = mpsc::unbounded_channel();
        let mut delivery = Delivery::replaying(
            Client {
                id: 1,
                messages,
                close,
            },
            VecDeque::from([HistoryLine {
                stream: OutputStream::Stdout,
                data: b"history\n".to_vec(),
            }]),
        );
        assert!(delivery.enqueue(OutputChunk::new(OutputStream::Stdout, b"live\n")));

        assert!(delivery.writer_ready());
        let BrokerMessage::Output { data_base64, .. } = outbound.recv().await.unwrap() else {
            panic!("expected replay output");
        };
        assert_eq!(protocol::decode_output(&data_base64).unwrap(), b"history\n");

        assert!(delivery.writer_ready());
        let BrokerMessage::Output { data_base64, .. } = outbound.recv().await.unwrap() else {
            panic!("expected live output");
        };
        assert_eq!(protocol::decode_output(&data_base64).unwrap(), b"live\n");
    }

    #[test]
    fn bounds_pending_live_output_by_wire_bytes() {
        let (messages, _) = mpsc::channel(CLIENT_WRITER_QUEUE);
        let (close, _) = mpsc::unbounded_channel();
        let mut delivery = Delivery::live(Client {
            id: 1,
            messages,
            close,
        });
        let output = OutputChunk::new(OutputStream::Stdout, &vec![b'x'; OUTPUT_CHUNK]);

        while delivery.enqueue(output.clone()) {}

        let pending_bytes = match &delivery.state {
            DeliveryState::Live { pending } | DeliveryState::Replaying { pending, .. } => {
                pending.wire_bytes
            }
        };
        assert!(pending_bytes <= CLIENT_BACKLOG_BYTES);
        assert!(!delivery.enqueue(output));
    }
}
