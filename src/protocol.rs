//! The broker's length-prefixed JSON wire protocol.

use std::fmt;
use std::io;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// The maximum number of bytes in one JSON frame payload.
pub const MAX_FRAME_SIZE: usize = 1024 * 1024;

/// A message a client may send to the broker.
///
/// Version two deliberately models a connection as a transport for zero or
/// more subscriptions.  It is not a lease on a process.  Frames stay exactly
/// as they were in v1 (bounded, length-prefixed JSON); only their vocabulary
/// changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    Submit {
        cwd: String,
        argv: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        group: Option<String>,
        #[serde(default)]
        policies: SubmitPolicies,
    },
    Subscribe {
        request_id: String,
        /// Replays events strictly after this cursor.  Omitted means replay
        /// every event the broker retains for the request/execution.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after: Option<u64>,
    },
    CancelRequest {
        request_id: String,
    },
    Unsubscribe {
        subscription_id: String,
    },
}

impl ClientMessage {
    /// Returns a validated submit request after checking transport-level
    /// invariants. Semantic policy validation belongs to the application.
    pub fn validate_submit(self) -> Result<SubmitRequest, ProtocolError> {
        match self {
            Self::Submit {
                cwd,
                argv,
                key,
                group,
                policies,
            } => {
                if cwd.is_empty() {
                    return Err(ProtocolError::InvalidSubmit(
                        "cwd must not be empty".to_owned(),
                    ));
                }
                if argv.is_empty() || argv[0].is_empty() {
                    return Err(ProtocolError::InvalidSubmit(
                        "argv must contain a non-empty executable".to_owned(),
                    ));
                }
                if key.as_deref().is_some_and(str::is_empty)
                    || group.as_deref().is_some_and(str::is_empty)
                {
                    return Err(ProtocolError::InvalidSubmit(
                        "key and group must not be empty when supplied".to_owned(),
                    ));
                }

                Ok(SubmitRequest {
                    cwd,
                    argv,
                    key,
                    group,
                    policies,
                })
            }
            _ => Err(ProtocolError::ExpectedSubmit),
        }
    }
}

/// A validated command-submission request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitRequest {
    pub cwd: String,
    pub argv: Vec<String>,
    pub key: Option<String>,
    pub group: Option<String>,
    pub policies: SubmitPolicies,
}

/// Execution-local policy configuration carried on a submit frame.
///
/// Scope policy is intentionally absent: a request cannot smuggle a
/// conflicting queue policy into a group it does not own.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SubmitPolicies {
    pub retry_limit: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kill_grace_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unobserved_grace_ms: Option<u64>,
}

/// Which command output stream produced a chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputStream {
    Stdout,
    Stderr,
}

/// Ordered event reported by the broker.
///
/// `sequence` is monotonically increasing within an Execution's event log.
/// This gives reconnecting subscribers a stable replay cursor while allowing
/// request-only events to remain correlated without inventing a second order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleEvent {
    pub sequence: u64,
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    #[serde(flatten)]
    pub kind: LifecycleEventKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum LifecycleEventKind {
    RequestReceived,
    RequestAttached,
    RequestPending,
    RequestAssigned,
    RequestDropped {
        reason: String,
    },
    RequestSuperseded {
        by_request_id: String,
    },
    RequestRejected {
        reason: String,
    },
    ExecutionCreated,
    AttemptStarted {
        attempt_id: String,
    },
    Output {
        attempt_id: String,
        stream: OutputStream,
        data_base64: String,
    },
    RetryScheduled {
        after_ms: u64,
    },
    AttemptCompleted {
        attempt_id: String,
        outcome: WireOutcome,
    },
    ExecutionCompleted {
        outcome: WireOutcome,
    },
    RequestCompleted {
        outcome: WireOutcome,
    },
    ExecutionCancelling {
        reason: String,
    },
}

/// The client-visible semantic outcome. Process status is optional metadata;
/// termination reasons are never inferred from an incidental signal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum WireOutcome {
    Succeeded,
    Failed {
        code: Option<i32>,
        signal: Option<i32>,
    },
    TimedOut,
    Cancelled {
        reason: Option<String>,
    },
    Dropped {
        reason: String,
    },
    Superseded {
        by_request_id: String,
    },
    Rejected {
        reason: String,
    },
}

/// A message the broker may send to a client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BrokerMessage {
    Submitted {
        request_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        execution_id: Option<String>,
    },
    Subscribed {
        subscription_id: String,
        request_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        execution_id: Option<String>,
    },
    Event {
        event: LifecycleEvent,
    },
    Cancelled {
        request_id: String,
    },
    Unsubscribed {
        subscription_id: String,
    },
    Error {
        message: String,
    },
}

impl BrokerMessage {
    /// Creates an output event with its bytes encoded for JSON transport.
    pub fn output_event(
        sequence: u64,
        request_id: impl Into<String>,
        execution_id: impl Into<String>,
        attempt_id: impl Into<String>,
        stream: OutputStream,
        data: &[u8],
    ) -> Self {
        Self::Event {
            event: LifecycleEvent {
                sequence,
                request_id: request_id.into(),
                execution_id: Some(execution_id.into()),
                kind: LifecycleEventKind::Output {
                    attempt_id: attempt_id.into(),
                    stream,
                    data_base64: encode_output(data),
                },
            },
        }
    }
}

/// Encodes raw process output for a lifecycle output event.
pub fn encode_output(data: &[u8]) -> String {
    STANDARD.encode(data)
}

/// Decodes raw process output received in an output event.
pub fn decode_output(data_base64: &str) -> Result<Vec<u8>, ProtocolError> {
    STANDARD
        .decode(data_base64)
        .map_err(|error| ProtocolError::InvalidBase64(error.to_string()))
}

/// Errors while reading, writing, or validating protocol data.
#[derive(Debug)]
pub enum ProtocolError {
    Io(io::Error),
    Json(serde_json::Error),
    FrameTooLarge { length: usize },
    InvalidSubmit(String),
    ExpectedSubmit,
    InvalidBase64(String),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "protocol I/O error: {error}"),
            Self::Json(error) => write!(formatter, "invalid protocol JSON: {error}"),
            Self::FrameTooLarge { length } => write!(
                formatter,
                "protocol frame is {length} bytes; the maximum is {MAX_FRAME_SIZE} bytes"
            ),
            Self::InvalidSubmit(message) => {
                write!(formatter, "invalid submit request: {message}")
            }
            Self::ExpectedSubmit => write!(formatter, "expected a submit request"),
            Self::InvalidBase64(message) => write!(formatter, "invalid output base64: {message}"),
        }
    }
}

impl std::error::Error for ProtocolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for ProtocolError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for ProtocolError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

/// Reads one bounded, big-endian length-prefixed JSON payload.
pub async fn read_payload<R>(reader: &mut R) -> Result<Vec<u8>, ProtocolError>
where
    R: AsyncRead + Unpin,
{
    let length = reader.read_u32().await? as usize;
    if length > MAX_FRAME_SIZE {
        return Err(ProtocolError::FrameTooLarge { length });
    }

    let mut payload = vec![0; length];
    reader.read_exact(&mut payload).await?;
    Ok(payload)
}

/// Writes one bounded, big-endian length-prefixed JSON payload.
pub async fn write_payload<W>(writer: &mut W, payload: &[u8]) -> Result<(), ProtocolError>
where
    W: AsyncWrite + Unpin,
{
    if payload.len() > MAX_FRAME_SIZE {
        return Err(ProtocolError::FrameTooLarge {
            length: payload.len(),
        });
    }

    writer.write_u32(payload.len() as u32).await?;
    writer.write_all(payload).await?;
    writer.flush().await?;
    Ok(())
}

/// Reads and deserializes one protocol message.
pub async fn read_frame<R, T>(reader: &mut R) -> Result<T, ProtocolError>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    Ok(serde_json::from_slice(&read_payload(reader).await?)?)
}

/// Serializes and writes one protocol message.
pub async fn write_frame<W, T>(writer: &mut W, message: &T) -> Result<(), ProtocolError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let payload = serde_json::to_vec(message)?;
    write_payload(writer, &payload).await
}

/// Convenience name for [`read_frame`].
pub async fn read_message<R, T>(reader: &mut R) -> Result<T, ProtocolError>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    read_frame(reader).await
}

/// Convenience name for [`write_frame`].
pub async fn write_message<W, T>(writer: &mut W, message: &T) -> Result<(), ProtocolError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    write_frame(writer, message).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn frames_round_trip_messages() {
        let (mut writer, mut reader) = duplex(4096);
        let message = ClientMessage::Submit {
            cwd: "/tmp/work".to_owned(),
            argv: vec!["command".to_owned(), "hello world".to_owned()],
            key: None,
            group: Some("builds".to_owned()),
            policies: SubmitPolicies::default(),
        };

        write_frame(&mut writer, &message).await.unwrap();
        assert_eq!(
            read_frame::<_, ClientMessage>(&mut reader).await.unwrap(),
            message
        );
    }

    #[tokio::test]
    async fn rejects_oversized_incoming_frame_before_allocating() {
        let (mut writer, mut reader) = duplex(16);
        writer.write_u32((MAX_FRAME_SIZE + 1) as u32).await.unwrap();

        assert!(matches!(
            read_payload(&mut reader).await,
            Err(ProtocolError::FrameTooLarge { length }) if length == MAX_FRAME_SIZE + 1
        ));
    }

    #[tokio::test]
    async fn rejects_oversized_outgoing_frame() {
        let (mut writer, _) = duplex(16);
        let payload = vec![b'x'; MAX_FRAME_SIZE + 1];

        assert!(matches!(
            write_payload(&mut writer, &payload).await,
            Err(ProtocolError::FrameTooLarge { length }) if length == MAX_FRAME_SIZE + 1
        ));
    }

    #[test]
    fn output_events_preserve_arbitrary_bytes() {
        let bytes = [0, b'\n', 0xff, b'a'];
        let message = BrokerMessage::output_event(
            7,
            "request-1",
            "execution-1",
            "attempt-1",
            OutputStream::Stderr,
            &bytes,
        );

        let BrokerMessage::Event { event } = &message else {
            panic!("expected output message");
        };
        let LifecycleEventKind::Output { data_base64, .. } = &event.kind else {
            panic!("expected output event");
        };
        assert_eq!(decode_output(data_base64).unwrap(), bytes);
        assert_eq!(
            serde_json::to_string(&message).unwrap(),
            r#"{"type":"event","event":{"sequence":7,"request_id":"request-1","execution_id":"execution-1","event":"output","attempt_id":"attempt-1","stream":"stderr","data_base64":"AAr/YQ=="}}"#
        );
    }

    #[test]
    fn validates_submit_fields() {
        let invalid = ClientMessage::Submit {
            cwd: String::new(),
            argv: vec!["command".to_owned()],
            key: None,
            group: None,
            policies: SubmitPolicies::default(),
        };
        assert!(matches!(
            invalid.validate_submit(),
            Err(ProtocolError::InvalidSubmit(_))
        ));

        let valid = ClientMessage::Submit {
            cwd: "/tmp/work".to_owned(),
            argv: vec!["command".to_owned()],
            key: None,
            group: None,
            policies: SubmitPolicies::default(),
        };
        assert_eq!(valid.validate_submit().unwrap().argv, ["command"]);
    }

    #[test]
    fn subscription_cursor_is_optional_and_wire_stable() {
        let message = ClientMessage::Subscribe {
            request_id: "request-1".into(),
            after: Some(41),
        };
        assert_eq!(
            serde_json::to_string(&message).unwrap(),
            r#"{"type":"subscribe","request_id":"request-1","after":41}"#
        );
    }
}
