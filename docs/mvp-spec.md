# foxrun: local long-running process deduplicator

## Goal

Build `foxrun`, a macOS/Linux CLI that lets concurrent local clients share one
long-running command process when they request the same working directory and
exact command argument vector. Clients receive that process's stdout, stderr,
and exit event. A process remains alive while clients are attached and is
stopped after a configurable idle grace period once its last client disconnects.

`foxrun` is a single Rust binary. It runs normally as the user-facing client
and contains a hidden `foxrun broker` subcommand used to host the local broker.

## Scope

Initial support includes:

- macOS and Linux only, using Unix-domain stream sockets.
- `--cwd <PATH>`, defaulting to the invoking client's current working directory.
- `--tail <LINES>`, omitted by default, to request replay of recent output
  lines before live streaming begins.
- `--broker-timeout <DURATION>`, defaulting to `5s`, as the idle grace period
  for the requested process.
- An explicit `--` CLI boundary followed by a command and its exact argument
  vector.
- One local broker that can own multiple independently deduplicated processes.
- Per-process runtime configuration: every successful acquire updates that
  process's `broker_timeout` to the value supplied by the newest client.

## User interface

### Client command

```text
foxrun [--cwd <PATH>] [--tail <LINES>] [--broker-timeout <DURATION>] -- <COMMAND> [ARG]...
```

Examples:

```sh
foxrun -- tsc --watch --pretty false
foxrun --cwd ./packages/web --tail 50 --broker-timeout 10s -- pnpm dev
foxrun -- my-command "hello world"
```

Rules:

- `--` is required. Everything after it is passed as the command argv without
  shell parsing or re-quoting.
- At least one element must follow `--`; its first element is the executable.
- The client resolves `--cwd` to an absolute canonical directory before
  connecting. It reports an error if the directory does not exist or cannot be
  canonicalized.
- `--tail` is a non-negative base-10 integer. Omission requests no history;
  `--tail 0` is equivalent to no history.
- `--broker-timeout` accepts a positive human-readable duration accepted by the
  chosen CLI parser, with `5s` as the default. The displayed help must document
  supported units.
- Argument boundaries are preserved. For example, shell input
  `-- node script.js "hello world"` is sent as
  `["node", "script.js", "hello world"]`.
- The client forwards broker stdout events to its stdout and stderr events to
  its stderr as raw bytes. It must not add prefixes or alter output bytes.
- On a command exit event, the client exits with the reported numeric exit code.
  If the command ended by signal, it exits with conventional status `128 +
  signal_number` on Unix.
- Ctrl-C, SIGTERM, or an unrecoverable client-side connection error closes only
  this client's broker connection. It does not directly stop the shared
  process. The client exits with the normal signal-derived status.

### Hidden subcommands

`foxrun broker --socket <PATH>` is hidden from normal help and is launched only
by the client. It listens on the supplied Unix socket path.

## Process identity and configuration

Each command is represented by a broker process record keyed by the structured
value:

```json
{
  "cwd": "/canonical/absolute/path",
  "argv": ["executable", "arg1", "arg2"]
}
```

Serialize this structured value with JSON and hash it for use as an internal map
key. Do not create the key by joining argv with spaces. `--tail` and
`--broker-timeout` are not part of the key.

The initial release does not expose environment configuration. The command
inherits the broker environment, which is the environment of the first client
that starts that broker. Environment is intentionally absent from the initial
process key and will become an explicit keyed configuration feature later.

The record contains:

- canonical `cwd` and exact `argv`;
- current `broker_timeout`;
- child PID/process-group identity and state;
- active attached client connections;
- bounded output history;
- an optional idle-shutdown timer.

On every `acquire` for an existing record, the broker immediately overwrites
that record's `broker_timeout` with the requesting client's value. If the
process is idle with a timer pending, a newly attached client cancels the timer.

## Broker lifecycle and startup

Use one broker per local user, not one broker per deduplicated process.

1. The client derives a stable per-user runtime directory:
   - Linux: `$XDG_RUNTIME_DIR/foxrun` when `XDG_RUNTIME_DIR` is set; otherwise
     `$TMPDIR/foxrun-<uid>`.
   - macOS: `$TMPDIR/foxrun-<uid>`.
2. Create this directory with mode `0700`. The broker socket path is
   `<runtime-dir>/broker.sock`; keep it below the Unix socket path-length limit.
3. The client first attempts to connect to the socket.
4. On `ENOENT` or `ECONNREFUSED`, it obtains an exclusive startup lock at
   `<runtime-dir>/broker.lock`, then retries the connection. If the second
   attempt still fails, it removes only the stale socket file, spawns
   `foxrun broker --socket <path>`, and retries connection until a short fixed
   startup deadline (2 seconds).
5. The spawned broker detaches from the client session, uses null stdin, and
   writes diagnostic logging only to a broker log file in the runtime directory.
   It must remain running after the launching client exits.
6. The broker removes the listening socket on orderly exit. A later client also
   handles a stale socket according to step 4.

The broker exits after it has no connected clients, no live process records,
and no pending idle timers.

## Connection protocol

Use one Unix stream socket listener. Each accepted connection is one distinct
client attachment/lease; the listener path is not itself a lease.

Messages are length-prefixed frames: a 4-byte unsigned big-endian payload
length followed by UTF-8 JSON. Limit a single JSON payload to 1 MiB. Output
bytes are represented with base64 to keep framing unambiguous.

Client-to-broker messages:

```json
{
  "type": "acquire",
  "cwd": "/canonical/absolute/path",
  "argv": ["tsc", "--watch"],
  "tail_lines": 50,
  "broker_timeout_ms": 5000
}
```

The first client message must be `acquire`. One connection acquires exactly one
process record in this release. The connection staying open is the lease; no
heartbeat is required for a local Unix socket.

Broker-to-client messages:

```json
{ "type": "attached", "reused": true }
{ "type": "output", "stream": "stdout", "data_base64": "..." }
{ "type": "output", "stream": "stderr", "data_base64": "..." }
{ "type": "exit", "code": 0, "signal": null }
{ "type": "error", "message": "..." }
```

The broker sends `attached` before any replay or live output. It then sends the
requested tail replay, then live output. It sends exactly one terminal `exit`
message to every client attached when the command ends, then closes those
connections after queued output is flushed.

Malformed frames, frames over the limit, an invalid acquire request, or an
acquire for an unstartable command receive an `error` message and a closed
connection. The client presents the error on stderr and exits nonzero.

## Command execution and ownership

The broker starts commands without a shell, equivalent to:

```rust
Command::new(&argv[0]).args(&argv[1..]).current_dir(&cwd)
```

Each command runs as a direct child of the broker in its own Unix process
group/session. The broker captures the command's stdout and stderr.

- The broker performs orderly termination when an idle timer expires:
  send `SIGTERM` to the command process group, wait 1 second, then send
  `SIGKILL` if it remains alive.
- On normal broker shutdown, it applies the same termination sequence to every
  live command process group before exiting.
- The broker records the command exit status. An exited record is removed after
  broadcasting its terminal event; a future acquire starts a new command
  instance.

An unexpected broker crash may orphan a command process. Crash-time orphan
discovery and cleanup are deliberately outside the initial release; users may
inspect and terminate such processes with normal OS tools.

## Output and history

The broker captures stdout and stderr independently and immediately forwards
every received byte chunk to all currently attached clients as `output` events.

Maintain a per-process bounded history ring of the most recent **1,000 complete
lines** across both streams, tagged with their originating stream in the order
the broker receives completed lines. A partial line is forwarded live but is
not available to a later `--tail` request until it is completed.

For `--tail N`, replay the final `min(N, 1000)` stored lines in retained order,
including their line terminators, before delivering future output. There is no
history replay when `--tail` is omitted or zero.

If a slow client cannot accept output within a bounded per-client outbound
queue of 1 MiB, close that client connection. Its eventual socket close releases
its lease. Other clients and the shared command remain unaffected.

## Client attachment and idle shutdown

When an `acquire` succeeds, the broker adds that connection to the process
record's client set. A disconnect, EOF, or connection write failure removes it
from the set automatically.

When a process record transitions to zero clients, arm an idle timer using its
current `broker_timeout`. If a client attaches before expiry, cancel the timer.
At expiry, terminate the command process group and remove the record once its
terminal exit is observed.

An ordinary command that exits while clients remain attached broadcasts its
exit status immediately. Those client connections then close; later invocations
for the same key start a fresh process.

## Non-goals

- Remote brokers, TCP networking, distributed consensus, and cross-machine
  sharing.
- Shared stdin or selecting a client as an stdin writer.
- Environment-variable overrides or environment-aware process keys.
- Restart-on-exit policy, persistent process state, and durable output history.
- Automatic cleanup of processes orphaned by an unexpected broker crash.

## Validation and test plan

Establish a Cargo workspace/project with formatting, clippy, unit tests, and
integration tests runnable through documented commands.

Automated tests must cover:

- Parsing `--cwd`, `--tail`, `--broker-timeout`, and required `--` command
  boundary; argv values containing spaces remain one argument.
- Two clients with the same canonical cwd and argv attach to one process; a
  different cwd or argv starts a distinct process.
- A second client using `--broker-timeout 10s` updates an existing record from
  `5s` to `10s`.
- Client disconnect removes its lease; the process survives during the grace
  period and is terminated after the final lease's configured timeout.
- A newly connected client cancels an armed idle timer.
- `--tail N` replays the requested final lines in stream order before future
  live output; no tail is replayed by default.
- stdout and stderr reach every attached client without cross-stream byte
  corruption.
- Command exit is sent to all attached clients and the client returns its exit
  status.
- A stale socket file is recovered safely and concurrent initial clients start
  only one broker.
- Orderly broker shutdown terminates every active command process group, with
  platform-appropriate integration coverage on macOS and Linux.

## Success criteria

- `cargo build --release` produces a runnable `foxrun` binary for macOS and
  Linux.
- Running two identical commands through `foxrun` produces one shared child
  process, and both callers receive its subsequent stdout/stderr and terminal
  exit status.
- `foxrun --cwd . --tail 50 --broker-timeout 10s -- tsc --watch` works end to
  end, including history replay for a later client and automatic process
  shutdown after its final client disconnects.
- New clients deterministically overwrite the matching process record's idle
  timeout.
- The broker terminates command process groups on normal shutdown and on idle
  timeout.
- `cargo fmt --check`, `cargo clippy -- -D warnings`, and the complete unit and
  integration test suite pass.
