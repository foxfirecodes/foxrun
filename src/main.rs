mod application;
mod broker;
mod client;
mod domain;
mod protocol;
mod registries;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, ValueEnum};

/// Run a command through the local foxrun broker.
#[derive(Debug, Parser)]
#[command(name = "foxrun", version, about, help_template = HELP)]
struct Cli {
    /// Directory in which to run the command (defaults to the current directory).
    #[arg(long, value_name = "PATH", hide = true)]
    cwd: Option<PathBuf>,

    /// Set the execution identity.
    #[arg(long, value_name = "KEY")]
    key: Option<String>,
    /// Share concurrency and rate limits across commands.
    #[arg(long, value_name = "GROUP")]
    group: Option<String>,
    /// Allow at most N executions in a group at once.
    #[arg(long, value_name = "N", value_parser = parse_positive_usize)]
    max_concurrency: Option<usize>,
    /// How to handle requests that cannot run immediately.
    #[arg(long, value_name = "POLICY", value_enum)]
    queue: Option<QueueArg>,
    /// Start at most N executions per duration, for example 10/1m.
    #[arg(long, value_name = "N/DURATION", value_parser = parse_rate_limit)]
    rate_limit: Option<RateLimitArg>,
    /// Terminate an execution after this duration.
    #[arg(long, value_name = "DURATION", value_parser = parse_duration)]
    timeout: Option<Duration>,
    /// Force kill if graceful termination takes this long.
    #[arg(long, value_name = "DURATION", value_parser = parse_duration)]
    kill_after: Option<Duration>,
    /// Retry a failed execution up to N times.
    #[arg(long, value_name = "N")]
    retries: Option<u32>,
    /// Retry only for these comma-separated exit codes.
    #[arg(long, value_delimiter = ',', value_name = "CODES")]
    retry_on: Vec<i32>,
    /// Never retry these comma-separated exit codes.
    #[arg(long, value_delimiter = ',', value_name = "CODES")]
    no_retry_on: Vec<i32>,
    /// Base delay between retry attempts.
    #[arg(long, value_name = "DURATION", value_parser = parse_duration)]
    retry_delay: Option<Duration>,
    /// Retry delay policy: fixed or exponential.
    #[arg(long, value_name = "POLICY", value_enum)]
    backoff: Option<RetryBackoffArg>,
    /// Randomize retry delays by up to this percentage.
    #[arg(long, value_name = "PERCENT", value_parser = parse_percent)]
    jitter: Option<u16>,
    /// Emit machine-readable lifecycle events.
    #[arg(long)]
    json: bool,

    /// Internal broker mode.
    #[arg(long, hide = true)]
    broker: bool,
    #[arg(long, hide = true)]
    socket: Option<PathBuf>,

    /// Command and exact arguments. This must follow `--`.
    #[arg(
        last = true,
        required_unless_present = "broker",
        allow_hyphen_values = true,
        value_name = "ARGV"
    )]
    argv: Vec<String>,
}

const HELP: &str = "Usage: foxrun [OPTIONS] -- COMMAND [ARGS]...\n\nExecution identity:\n  --key <KEY>                   Set the execution identity.\n                                Default: canonical working directory + command.\n\nConcurrency:\n  --group <GROUP>               Share concurrency and rate limits across commands.\n  --max-concurrency <N>         Allow at most N executions in a group at once.\n\n  --queue <POLICY>              How to handle requests that cannot run immediately.\n                                fifo      Queue every request in arrival order.\n                                latest    Keep only the newest waiting request.\n                                drop      Discard new requests while busy.\n                                replace   Cancel the running request for the newest.\n                                Default: attach to an identical running command.\n\nRate limiting:\n  --rate-limit <N>/<DURATION>   Start at most N executions per duration.\n                                Example: --rate-limit 10/1m\n\nTimeout:\n  --timeout <DURATION>          Terminate an execution after this duration.\n  --kill-after <DURATION>       Force kill if graceful termination takes this long.\n\nRetries:\n  --retries <N>                 Retry a failed execution up to N times.\n  --retry-on <CODES>            Retry only for these exit codes.\n  --no-retry-on <CODES>         Never retry these exit codes.\n  --retry-delay <DURATION>      Base delay between retry attempts.\n  --backoff <POLICY>            Retry delay policy: fixed or exponential.\n  --jitter <PERCENT>            Randomize retry delays by up to this percentage.\n\nOutput:\n  --json                        Emit machine-readable lifecycle events.\n";

#[derive(Debug, Clone, Copy, ValueEnum)]
enum QueueArg {
    Fifo,
    Latest,
    Drop,
    Replace,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum RetryBackoffArg {
    Fixed,
    Exponential,
}

#[derive(Debug, Clone, Copy)]
struct RateLimitArg {
    max_starts: usize,
    per: Duration,
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let duration = humantime::parse_duration(value).map_err(|error| error.to_string())?;
    if duration.is_zero() {
        return Err("duration must be greater than zero".into());
    }
    Ok(duration)
}
fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let n: usize = value
        .parse()
        .map_err(|_| "must be a positive integer".to_owned())?;
    if n == 0 {
        Err("must be greater than zero".into())
    } else {
        Ok(n)
    }
}
fn parse_rate_limit(value: &str) -> Result<RateLimitArg, String> {
    let (count, duration) = value
        .split_once('/')
        .ok_or_else(|| "expected N/DURATION (for example 10/1m)".to_owned())?;
    Ok(RateLimitArg {
        max_starts: parse_positive_usize(count)?,
        per: parse_duration(duration)?,
    })
}
fn parse_percent(value: &str) -> Result<u16, String> {
    let percent: u16 = value
        .parse()
        .map_err(|_| "must be an integer percentage".to_owned())?;
    if percent > 100 {
        Err("must be between 0 and 100".into())
    } else {
        Ok(percent)
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("foxrun: {error:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    if cli.broker {
        let socket = cli
            .socket
            .ok_or_else(|| anyhow::anyhow!("internal broker requires --socket"))?;
        return broker::run(socket).await;
    }
    client::run(to_submit_options(cli)).await
}

fn to_submit_options(args: Cli) -> client::SubmitOptions {
    client::SubmitOptions {
        cwd: args.cwd,
        argv: args.argv,
        key: args.key,
        group: args.group,
        contention: args.queue.map(|q| match q {
            QueueArg::Fifo => protocol::ContentionMode::Queue,
            QueueArg::Latest => protocol::ContentionMode::Latest,
            QueueArg::Drop => protocol::ContentionMode::Drop,
            QueueArg::Replace => protocol::ContentionMode::Replace,
        }),
        max_concurrency: args.max_concurrency,
        rate_limit: args.rate_limit.map(|r| protocol::WireRateLimit {
            max_starts: r.max_starts,
            per_ms: duration_millis(r.per),
        }),
        retry_limit: args.retries.unwrap_or(0),
        retry_on: (!args.retry_on.is_empty()).then_some(args.retry_on),
        no_retry_on: args.no_retry_on,
        retry_delay: args.retry_delay,
        retry_backoff_exponential: matches!(args.backoff, Some(RetryBackoffArg::Exponential)),
        retry_jitter_basis_points: args.jitter.unwrap_or(0) * 100,
        attempt_timeout: args.timeout,
        kill_grace: args.kill_after.unwrap_or(Duration::from_secs(1)),
        unobserved_grace: None,
        json: args.json,
    }
}
fn duration_millis(value: Duration) -> u64 {
    value.as_millis().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_old_shape_and_preserves_argv() {
        let cli = Cli::try_parse_from([
            "foxrun",
            "--key",
            "web",
            "--queue",
            "latest",
            "--rate-limit",
            "10/1m",
            "--",
            "echo",
            "hello world",
        ])
        .unwrap();
        assert_eq!(cli.key.as_deref(), Some("web"));
        assert!(matches!(cli.queue, Some(QueueArg::Latest)));
        assert_eq!(cli.argv, ["echo", "hello world"]);
    }
    #[test]
    fn validates_rates_and_percent() {
        assert!(Cli::try_parse_from(["foxrun", "--rate-limit", "0/1m", "--", "echo"]).is_err());
        assert!(Cli::try_parse_from(["foxrun", "--jitter", "101", "--", "echo"]).is_err());
    }
}
