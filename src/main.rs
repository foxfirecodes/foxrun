mod application;
mod broker;
mod client;
mod domain;
mod protocol;
mod registries;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Result, bail};
use clap::{Parser, Subcommand};

/// Share a local long-running command with other invocations of the same argv.
#[derive(Debug, Parser)]
#[command(name = "foxrun", version, about, subcommand_negates_reqs = true)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Directory in which to run the command (defaults to the current directory).
    #[arg(long, value_name = "PATH")]
    cwd: Option<PathBuf>,

    /// Replay this many complete output lines before live output.
    #[arg(long, value_name = "LINES", default_value_t = 0usize)]
    tail: usize,

    /// Stop an unobserved command after this duration. Supported units include
    /// ns, us, ms, s, m, h, d, w, month, and year.
    #[arg(
        long,
        value_name = "DURATION",
        default_value = "5s",
        value_parser = parse_duration
    )]
    broker_timeout: Duration,

    /// Command and exact arguments. This must follow `--`.
    #[arg(last = true, required = true, allow_hyphen_values = true)]
    argv: Vec<String>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Internal broker process; not intended for direct use.
    #[command(hide = true)]
    Broker {
        #[arg(long)]
        socket: PathBuf,
    },
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let duration = humantime::parse_duration(value).map_err(|error| error.to_string())?;
    if duration.is_zero() {
        return Err("duration must be greater than zero".into());
    }
    Ok(duration)
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("foxrun: {error:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    if let Some(Command::Broker { socket }) = cli.command {
        return broker::run(socket).await;
    }

    if cli.argv.is_empty() {
        bail!("a command is required after `--`\n\nTry `foxrun -- <COMMAND> [ARG]...`");
    }
    client::run(client::ClientOptions {
        cwd: cli.cwd,
        argv: cli.argv,
        tail_lines: cli.tail,
        broker_timeout: cli.broker_timeout,
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_client_options_and_preserves_argv() {
        let cli = Cli::try_parse_from([
            "foxrun",
            "--cwd",
            ".",
            "--tail",
            "50",
            "--broker-timeout",
            "10s",
            "--",
            "node",
            "script.js",
            "hello world",
        ])
        .unwrap();
        assert_eq!(cli.cwd, Some(PathBuf::from(".")));
        assert_eq!(cli.tail, 50);
        assert_eq!(cli.broker_timeout, Duration::from_secs(10));
        assert_eq!(cli.argv, ["node", "script.js", "hello world"]);
    }

    #[test]
    fn rejects_missing_boundary() {
        assert!(Cli::try_parse_from(["foxrun", "echo", "hello"]).is_err());
    }

    #[test]
    fn rejects_zero_timeout() {
        assert!(Cli::try_parse_from(["foxrun", "--broker-timeout", "0s", "--", "echo"]).is_err());
    }
}
