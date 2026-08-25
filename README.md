# foxrun

`foxrun` lets local clients share one long-running command when they use the
same canonical working directory and exact argument list.

```sh
cargo run -- -- tsc --watch --pretty false
cargo run -- --cwd ./packages/web --tail 50 --broker-timeout 10s -- pnpm dev
```

The `--` boundary is required. `--broker-timeout` accepts positive durations
such as `250ms`, `5s`, `10m`, and `1h`. When the last client exits, foxrun
keeps the command alive for that duration before stopping its process group.

The binary runs a local broker automatically. `foxrun broker --socket <PATH>`
is an internal subcommand and is hidden from normal help.

## Checks

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```
