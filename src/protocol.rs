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
/// A connection must send exactly one [`ClientMessage::Acquire`] as its first
/// message. There are no further client-to-broker messages in the MVP.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    Acquire {
        cwd: String,
        argv: Vec<String>,
        tail_lines: usize,
        broker_timeout_ms: u64,
    },
}

impl ClientMessage {
    /// Returns the acquire request after checking protocol-level invariants.
    pub fn validate(self) -> Result<AcquireRequest, ProtocolError> {
        match self {
            Self::Acquire {
                cwd,
                argv,
                tail_lines,
                broker_timeout_ms,
            } => {
                if cwd.is_empty() {
                    return Err(ProtocolError::InvalidAcquire(
                        "cwd must not be empty".to_owned(),
                    ));
                }
                if argv.is_empty() || argv[0].is_empty() {
                    return Err(ProtocolError::InvalidAcquire(
                        "argv must contain a non-empty executable".to_owned(),
                    ));
                }
                if broker_timeout_ms == 0 {
                    return Err(ProtocolError::InvalidAcquire(
                        "broker_timeout_ms must be positive".to_owned(),
                    ));
                }

                Ok(AcquireRequest {
                    cwd,
                    argv,
                    tail_lines,
                    broker_timeout_ms,
                })
            }
        }
    }
}

/// A validated command-acquisition request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquireRequest {
    pub cwd: String,
    pub argv: Vec<String>,
    pub tail_lines: usize,
    pub broker_timeout_ms: u64,
}

/// Which command output stream produced a chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputStream {
    Stdout,
    Stderr,
}

/// A message the broker may send to a client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BrokerMessage {
    Attached {
        reused: bool,
    },
    Output {
        stream: OutputStream,
        data_base64: String,
    },
    Exit {
        code: Option<i32>,
        signal: Option<i32>,
    },
    Error {
        message: String,
    },
}

impl BrokerMessage {
    /// Creates an output event with its bytes encoded for JSON transport.
    pub fn output(stream: OutputStream, data: &[u8]) -> Self {
        Self::Output {
            stream,
            data_base64: encode_output(data),
        }
    }
}

/// Encodes raw process output for an [`BrokerMessage::Output`] event.
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
    InvalidAcquire(String),
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
            Self::InvalidAcquire(message) => {
                write!(formatter, "invalid acquire request: {message}")
            }
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

/// Backwards-compatible name for [`read_frame`].
pub async fn read_message<R, T>(reader: &mut R) -> Result<T, ProtocolError>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    read_frame(reader).await
}

/// Backwards-compatible name for [`write_frame`].
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
        let message = ClientMessage::Acquire {
            cwd: "/tmp/work".to_owned(),
            argv: vec!["command".to_owned(), "hello world".to_owned()],
            tail_lines: 50,
            broker_timeout_ms: 5_000,
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
        let message = BrokerMessage::output(OutputStream::Stderr, &bytes);

        let BrokerMessage::Output { data_base64, .. } = &message else {
            panic!("expected output message");
        };
        assert_eq!(decode_output(data_base64).unwrap(), bytes);
        assert_eq!(
            serde_json::to_string(&message).unwrap(),
            r#"{"type":"output","stream":"stderr","data_base64":"AAr/YQ=="}"#
        );
    }

    #[test]
    fn validates_acquire_fields() {
        let invalid = ClientMessage::Acquire {
            cwd: String::new(),
            argv: vec!["command".to_owned()],
            tail_lines: 0,
            broker_timeout_ms: 1,
        };
        assert!(matches!(
            invalid.validate(),
            Err(ProtocolError::InvalidAcquire(_))
        ));

        let valid = ClientMessage::Acquire {
            cwd: "/tmp/work".to_owned(),
            argv: vec!["command".to_owned()],
            tail_lines: 0,
            broker_timeout_ms: 1,
        };
        assert_eq!(valid.validate().unwrap().argv, ["command"]);
    }
}
