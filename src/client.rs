use std::fs::{self, OpenOptions};
use std::io;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use fs2::FileExt;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::time::{Instant, sleep};

use crate::protocol::{
    BrokerMessage, ClientMessage, OutputStream, ProtocolError, decode_output, read_frame,
    write_frame,
};

const STARTUP_DEADLINE: Duration = Duration::from_secs(2);
const RETRY_DELAY: Duration = Duration::from_millis(25);

#[derive(Debug)]
pub struct ClientOptions {
    pub cwd: Option<PathBuf>,
    pub argv: Vec<String>,
    pub tail_lines: usize,
    pub broker_timeout: Duration,
}

pub async fn run(options: ClientOptions) -> Result<()> {
    let cwd = canonical_directory(options.cwd.as_deref())?;
    let runtime_dir = runtime_dir()?;
    let socket = runtime_dir.join("broker.sock");
    check_socket_path(&socket)?;
    let broker_timeout_ms = options
        .broker_timeout
        .as_millis()
        .try_into()
        .map_err(|_| anyhow!("--broker-timeout is too large"))?;
    let acquire = ClientMessage::Acquire {
        cwd: cwd.to_string_lossy().into_owned(),
        argv: options.argv,
        tail_lines: options.tail_lines,
        broker_timeout_ms,
    };
    let deadline = Instant::now() + STARTUP_DEADLINE;
    let mut last_error = None;

    loop {
        let mut stream = connect_or_start(&runtime_dir, &socket).await?;
        match send_acquire_and_wait_for_attach(&mut stream, &acquire).await? {
            AcquireResult::Attached => return stream_messages(&mut stream).await,
            AcquireResult::ConnectionClosed(error) => {
                last_error = Some(error);
                if Instant::now() >= deadline {
                    bail!(
                        "broker did not accept acquire within {}: {}",
                        humantime::format_duration(STARTUP_DEADLINE),
                        last_error.expect("connection error was recorded")
                    );
                }
                sleep(RETRY_DELAY).await;
            }
        }
    }
}

enum AcquireResult {
    Attached,
    /// The broker can close a just-established connection while it exits. No
    /// acquire was acknowledged, so it is safe to connect again.
    ConnectionClosed(ProtocolError),
}

async fn send_acquire_and_wait_for_attach<S>(
    stream: &mut S,
    acquire: &ClientMessage,
) -> Result<AcquireResult>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if let Err(error) = write_frame(stream, acquire).await {
        return match error {
            ProtocolError::Io(error) => {
                Ok(AcquireResult::ConnectionClosed(ProtocolError::Io(error)))
            }
            error => Err(error).context("could not send acquire request"),
        };
    }

    match read_frame::<_, BrokerMessage>(stream).await {
        Ok(BrokerMessage::Attached { .. }) => Ok(AcquireResult::Attached),
        Ok(BrokerMessage::Error { message }) => bail!("broker: {message}"),
        Ok(message) => bail!("broker sent {message:?} before attached"),
        Err(ProtocolError::Io(error)) => {
            Ok(AcquireResult::ConnectionClosed(ProtocolError::Io(error)))
        }
        Err(error) => Err(error).context("broker sent an invalid response"),
    }
}

fn canonical_directory(input: Option<&Path>) -> Result<PathBuf> {
    let path = match input {
        Some(path) => path.to_path_buf(),
        None => std::env::current_dir().context("could not determine current directory")?,
    };
    let canonical = fs::canonicalize(&path).with_context(|| {
        format!(
            "could not canonicalize working directory {}",
            path.display()
        )
    })?;
    if !canonical.is_dir() {
        bail!(
            "working directory {} is not a directory",
            canonical.display()
        );
    }
    Ok(canonical)
}

fn runtime_dir() -> Result<PathBuf> {
    #[cfg(target_os = "linux")]
    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    #[cfg(target_os = "macos")]
    let base = std::env::temp_dir();
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    compile_error!("foxrun supports macOS and Linux only");

    let uid = unsafe { libc::getuid() };
    let directory = if cfg!(target_os = "linux") && std::env::var_os("XDG_RUNTIME_DIR").is_some() {
        base.join("foxrun")
    } else {
        base.join(format!("foxrun-{uid}"))
    };
    fs::create_dir_all(&directory)
        .with_context(|| format!("could not create runtime directory {}", directory.display()))?;
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("could not secure runtime directory {}", directory.display()))?;
    Ok(directory)
}

fn check_socket_path(socket: &Path) -> Result<()> {
    // sockaddr_un leaves room for a trailing NUL; the usual portable limit is 107 bytes.
    if socket.as_os_str().as_encoded_bytes().len() > 107 {
        bail!("broker socket path is too long: {}", socket.display());
    }
    Ok(())
}

async fn connect_or_start(runtime_dir: &Path, socket: &Path) -> Result<UnixStream> {
    match UnixStream::connect(socket).await {
        Ok(stream) => return Ok(stream),
        Err(error) if should_start_broker(&error) => {}
        Err(error) => return Err(error).context("could not connect to broker"),
    }

    let lock_path = runtime_dir.join("broker.lock");
    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .with_context(|| format!("could not open broker startup lock {}", lock_path.display()))?;
    lock.lock_exclusive()
        .context("could not acquire broker startup lock")?;

    let result = async {
        match UnixStream::connect(socket).await {
            Ok(stream) => return Ok(stream),
            Err(error) if should_start_broker(&error) => {}
            Err(error) => return Err(error).context("could not connect to broker"),
        }
        remove_stale_socket(socket)?;
        spawn_broker(runtime_dir, socket)?;
        wait_for_broker(socket).await
    }
    .await;
    let _ = lock.unlock();
    result
}

fn should_start_broker(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(code) if code == libc::ENOENT || code == libc::ECONNREFUSED)
}

fn remove_stale_socket(socket: &Path) -> Result<()> {
    match fs::symlink_metadata(socket) {
        Ok(metadata) if metadata.file_type().is_socket() => fs::remove_file(socket)
            .with_context(|| format!("could not remove stale broker socket {}", socket.display())),
        Ok(_) => bail!("refusing to replace non-socket path {}", socket.display()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).context("could not inspect broker socket"),
    }
}

fn spawn_broker(runtime_dir: &Path, socket: &Path) -> Result<()> {
    let executable = std::env::current_exe().context("could not find foxrun executable")?;
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(runtime_dir.join("broker.log"))
        .context("could not open broker log")?;
    let log_err = log
        .try_clone()
        .context("could not duplicate broker log handle")?;
    let mut command = Command::new(executable);
    command
        .arg("broker")
        .arg("--socket")
        .arg(socket)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err));
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    command.spawn().context("could not start broker")?;
    Ok(())
}

async fn wait_for_broker(socket: &Path) -> Result<UnixStream> {
    let deadline = Instant::now() + STARTUP_DEADLINE;
    let mut last_error = None;
    while Instant::now() < deadline {
        match UnixStream::connect(socket).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
        sleep(RETRY_DELAY).await;
    }
    Err(anyhow!(
        "broker did not start within {}: {}",
        humantime::format_duration(STARTUP_DEADLINE),
        last_error
            .map(|error| error.to_string())
            .unwrap_or_else(|| "unknown error".into())
    ))
}

async fn stream_messages(stream: &mut UnixStream) -> Result<()> {
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    loop {
        tokio::select! {
            result = read_frame::<_, BrokerMessage>(stream) => {
                let message = result.context("broker connection closed unexpectedly")?;
                match message {
                    BrokerMessage::Attached { .. } => {},
                    BrokerMessage::Output { stream, data_base64 } => {
                        let data = decode_output(&data_base64)
                            .context("broker sent invalid output encoding")?;
                        use std::io::Write;
                        match stream {
                            OutputStream::Stdout => std::io::stdout().write_all(&data),
                            OutputStream::Stderr => std::io::stderr().write_all(&data),
                        }.context("could not write command output")?;
                        match stream {
                            OutputStream::Stdout => std::io::stdout().flush(),
                            OutputStream::Stderr => std::io::stderr().flush(),
                        }.context("could not flush command output")?;
                    },
                    BrokerMessage::Exit { code, signal } => {
                        let status = code.or_else(|| signal.map(|number| 128 + number)).unwrap_or(1);
                        std::process::exit(status);
                    },
                    BrokerMessage::Error { message } => bail!("broker: {message}"),
                }
            }
            _ = interrupt.recv() => {
                // The broker may have inherited a copy of this socket while
                // it detached during startup. Send a FIN explicitly so the
                // broker releases this lease even in that case.
                let _ = stream.shutdown().await;
                std::process::exit(130);
            }
            _ = terminate.recv() => {
                // See the SIGINT branch above.
                let _ = stream.shutdown().await;
                std::process::exit(143);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[test]
    fn rejects_non_directory() {
        let file = tempfile::NamedTempFile::new().unwrap();
        assert!(canonical_directory(Some(file.path())).is_err());
    }

    #[test]
    fn stale_socket_removal_refuses_regular_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("broker.sock");
        std::fs::File::create(&path).unwrap();
        assert!(remove_stale_socket(&path).is_err());
        assert!(path.exists());
    }

    #[tokio::test]
    async fn retries_when_broker_closes_before_attaching() {
        let (mut stream, mut broker) = duplex(4096);
        let server = tokio::spawn(async move {
            let _: ClientMessage = read_frame(&mut broker).await.unwrap();
        });
        let acquire = ClientMessage::Acquire {
            cwd: "/tmp".into(),
            argv: vec!["command".into()],
            tail_lines: 0,
            broker_timeout_ms: 1,
        };

        assert!(matches!(
            send_acquire_and_wait_for_attach(&mut stream, &acquire)
                .await
                .unwrap(),
            AcquireResult::ConnectionClosed(ProtocolError::Io(_))
        ));
        server.await.unwrap();
    }
}
