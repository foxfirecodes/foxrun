# foxrun

`foxrun` lets local invocations share one long-running command. Invocations
with the same canonical working directory and exact argument list attach to
the same process, so two terminals can watch the same `tsc`, dev server, or
other long-running command without starting it twice.

It supports macOS and Linux.

## Install

Install from this checkout with Rust and Cargo:

```sh
cargo install --path .
```

Or run it from the checkout:

```sh
cargo run -- -- tsc --watch --pretty false
```

## Use

Put the command after `--`. Everything after that boundary is passed directly
to the program; foxrun does not run a shell.

```sh
foxrun -- tsc --watch --pretty false
foxrun --cwd ./packages/web -- pnpm dev
foxrun --tail 50 -- pnpm dev
foxrun --broker-timeout 30s -- pnpm dev
```

Options:

- `--cwd <PATH>` runs the command in this directory. It defaults to the
  current directory.
- `--tail <LINES>` replays up to this many recent output lines before live
  output. The broker retains at most 1,000 complete lines per command.
- `--broker-timeout <DURATION>` keeps a command alive for this long after its
  last client disconnects. The default is `5s`; durations must be positive,
  such as `250ms`, `10s`, or `1m`.

Pressing Ctrl-C detaches only your terminal. It does not stop a command that
another foxrun client still uses.

## How it works

foxrun starts a private local broker when needed. The broker identifies a
command by its canonical working directory and its exact argument vector.
Matching requests reuse the process; any different directory or argument
starts another one.

The broker sends each command's stdout and stderr to every attached client.
When the final client leaves, it waits for that command's current timeout,
then stops the command's process group. If a client returns before the timeout
ends, the command stays alive. When a command exits, each attached client gets
the same exit status.

The broker runs under the local user and uses a Unix socket in that user's
runtime directory. Commands inherit the environment of the client that first
starts the broker, so use the same environment when sharing commands.

## Checks

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```
