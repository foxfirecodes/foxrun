#!/usr/bin/env bash
# End-to-end regression suite for the public foxrun CLI.
#
# This deliberately talks only to a separately running foxrun binary.  It does
# not use Rust test APIs or connect to the wire protocol itself, so it catches
# regressions at the CLI/client/broker/process boundary.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${FOXRUN_BIN:-$ROOT/target/debug/foxrun}"
KEEP_RUNTIME="${FOXRUN_KEEP_RUNTIME:-0}"
RUNTIME="$(mktemp -d "${TMPDIR:-/tmp}/foxrun-real-cli.XXXXXX")"
export XDG_RUNTIME_DIR="$RUNTIME"
SOCKET="$XDG_RUNTIME_DIR/foxrun/broker.sock"
BROKER_PID=""
CURRENT_TEST="startup"

fail() {
    printf '\nFAIL: %s: %s\n' "$CURRENT_TEST" "$*" >&2
    printf 'runtime retained at: %s\n' "$RUNTIME" >&2
    if [[ -f "$XDG_RUNTIME_DIR/foxrun/broker.log" ]]; then
        printf '%s\n' '--- broker.log ---' >&2
        sed -n '1,240p' "$XDG_RUNTIME_DIR/foxrun/broker.log" >&2
    fi
    exit 1
}

cleanup() {
    local status=$?
    trap - EXIT INT TERM
    # Commands run by foxrun are in separate process groups.  Every scenario
    # releases its command before returning; these are a final safety net.
    jobs -pr | while read -r pid; do kill -TERM "$pid" 2>/dev/null || true; done
    if [[ -n "$BROKER_PID" ]]; then
        kill -TERM "$BROKER_PID" 2>/dev/null || true
        wait "$BROKER_PID" 2>/dev/null || true
    fi
    if [[ $status -eq 0 && "$KEEP_RUNTIME" != 1 ]]; then
        rm -rf "$RUNTIME"
    else
        printf 'real CLI test artifacts: %s\n' "$RUNTIME" >&2
    fi
    exit "$status"
}
trap cleanup EXIT INT TERM

require() {
    command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}
require bash
require sh
require date

[[ -x "$BIN" ]] || fail "binary is not executable: $BIN (run cargo build, or set FOXRUN_BIN)"
mkdir -p "$(dirname "$SOCKET")"
"$BIN" --broker --socket "$SOCKET" >"$RUNTIME/broker.stdout" 2>"$RUNTIME/broker.log" &
BROKER_PID=$!

wait_for_file() {
    local path=$1 timeout_ms=${2:-3000} elapsed=0
    while [[ ! -e "$path" ]]; do
        (( elapsed >= timeout_ms )) && fail "timed out waiting for $path"
        sleep 0.01
        ((elapsed += 10))
    done
}

expect_absent_for() {
    local path=$1 duration_ms=$2 elapsed=0
    while (( elapsed < duration_ms )); do
        [[ ! -e "$path" ]] || fail "unexpectedly created: $path"
        sleep 0.01
        ((elapsed += 10))
    done
}

# Do this before the first client. Otherwise a fast client can race the
# supervised broker and launch an extra detached broker of its own.
wait_for_file "$SOCKET" 3000

expect_status() {
    local want=$1 got=$2 label=${3:-command}
    [[ "$got" == "$want" ]] || fail "$label exit status: expected $want, got $got"
}

expect_file_contains() {
    local path=$1 text=$2
    grep -F -- "$text" "$path" >/dev/null 2>&1 || {
        printf '%s\n' "--- $path ---" >&2
        sed -n '1,240p' "$path" >&2
        fail "expected $path to contain: $text"
    }
}

expect_file_not_contains() {
    local path=$1 text=$2
    if grep -F -- "$text" "$path" >/dev/null 2>&1; then
        fail "did not expect $path to contain: $text"
    fi
}

line_of() {
    local path=$1 text=$2
    grep -n -m1 -F -- "$text" "$path" | cut -d: -f1
}

expect_order() {
    local path=$1 first=$2 second=$3 one two
    one="$(line_of "$path" "$first")"
    two="$(line_of "$path" "$second")"
    [[ -n "$one" && -n "$two" && "$one" -lt "$two" ]] || fail "expected '$first' before '$second' in $path"
}

run_foreground() {
    # Usage: run_foreground LOG -- FOXRUN_ARGS...
    local log=$1; shift
    "$BIN" "$@" >"$log" 2>&1
}

start_client() {
    # Usage: start_client LOG -- FOXRUN_ARGS...; sets STARTED_PID.
    local log=$1; shift
    "$BIN" "$@" >"$log" 2>&1 &
    STARTED_PID=$!
}

wait_client() {
    local pid=$1
    wait "$pid"
    WAIT_STATUS=$?
}

announce() { CURRENT_TEST=$1; printf '== %s ==\n' "$CURRENT_TEST"; }

announce basic_stdout_stderr_and_exit
basic="$RUNTIME/basic.log"
run_foreground "$basic" -- sh -c 'echo stdout; echo stderr >&2; exit 7'; status=$?
expect_status 7 "$status"
expect_file_contains "$basic" stdout
expect_file_contains "$basic" stderr

announce json_lifecycle_and_output_order
json="$RUNTIME/json.log"
run_foreground "$json" --json -- sh -c 'echo json-out; echo json-err >&2'; status=$?
expect_status 0 "$status"
expect_file_contains "$json" '"event":"execution_created"'
expect_file_contains "$json" '"event":"output"'
expect_file_contains "$json" '"event":"attempt_completed"'
expect_file_contains "$json" '"event":"request_completed"'
expect_file_contains "$json" json-out
expect_file_contains "$json" json-err
expect_order "$json" '"event":"execution_created"' '"event":"output"'
expect_order "$json" '"event":"output"' '"event":"attempt_completed"'
expect_order "$json" '"event":"attempt_completed"' '"event":"request_completed"'

announce identical_request_reuses_one_execution
reuse_ready="$RUNTIME/reuse-ready"; reuse_count="$RUNTIME/reuse-count"
one_log="$RUNTIME/reuse-one.log"; two_log="$RUNTIME/reuse-two.log"
start_client "$one_log" -- sh -c 'printf x >> "$1"; touch "$2"; sleep .35; echo shared-finished' sh "$reuse_count" "$reuse_ready"; one=$STARTED_PID
wait_for_file "$reuse_ready"
start_client "$two_log" -- sh -c 'printf x >> "$1"; touch "$2"; sleep .35; echo shared-finished' sh "$reuse_count" "$reuse_ready"; two=$STARTED_PID
wait_client "$one"; expect_status 0 "$WAIT_STATUS" first_reuse_client
wait_client "$two"; expect_status 0 "$WAIT_STATUS" second_reuse_client
[[ "$(wc -c < "$reuse_count")" == 1 ]] || fail "identical requests started more than one process"
expect_file_contains "$one_log" shared-finished
expect_file_contains "$two_log" shared-finished

announce fifo_respects_group_capacity
fifo_release="$RUNTIME/fifo-release"; fifo_a_started="$RUNTIME/fifo-a-started"; fifo_b_started="$RUNTIME/fifo-b-started"
fifo_a_log="$RUNTIME/fifo-a.log"; fifo_b_log="$RUNTIME/fifo-b.log"
fifo_group="fifo-$RANDOM"
start_client "$fifo_a_log" --group "$fifo_group" --max-concurrency 1 --queue fifo --key fifo-a -- sh -c 'touch "$1"; while ! test -e "$2"; do sleep .01; done; echo A-end' sh "$fifo_a_started" "$fifo_release"; fifo_a=$STARTED_PID
wait_for_file "$fifo_a_started"
start_client "$fifo_b_log" --group "$fifo_group" --max-concurrency 1 --queue fifo --key fifo-b -- sh -c 'touch "$1"; echo B-end' sh "$fifo_b_started"; fifo_b=$STARTED_PID
expect_absent_for "$fifo_b_started" 150
touch "$fifo_release"
wait_client "$fifo_a"; expect_status 0 "$WAIT_STATUS" fifo_first
wait_client "$fifo_b"; expect_status 0 "$WAIT_STATUS" fifo_second
expect_file_contains "$fifo_a_log" A-end
expect_file_contains "$fifo_b_log" B-end

announce latest_supersedes_older_waiter
latest_group="latest-$RANDOM"; latest_release="$RUNTIME/latest-release"; latest_started="$RUNTIME/latest-started"
latest_old_marker="$RUNTIME/latest-old-ran"; latest_new_marker="$RUNTIME/latest-new-ran"
latest_first_log="$RUNTIME/latest-first.log"; latest_old_log="$RUNTIME/latest-old.log"; latest_new_log="$RUNTIME/latest-new.log"
start_client "$latest_first_log" --group "$latest_group" --max-concurrency 1 --queue latest --key latest-owner -- sh -c 'touch "$1"; while ! test -e "$2"; do sleep .01; done; echo first-end' sh "$latest_started" "$latest_release"; latest_first=$STARTED_PID
wait_for_file "$latest_started"
start_client "$latest_old_log" --group "$latest_group" --max-concurrency 1 --queue latest --key latest-key -- sh -c 'touch "$1"; echo OLD-WAITER-RAN' sh "$latest_old_marker"; latest_old=$STARTED_PID
start_client "$latest_new_log" --group "$latest_group" --max-concurrency 1 --queue latest --key latest-key -- sh -c 'touch "$1"; echo newest-ran' sh "$latest_new_marker"; latest_new=$STARTED_PID
touch "$latest_release"
wait_client "$latest_first"; expect_status 0 "$WAIT_STATUS" latest_first
wait_client "$latest_old"; old_status=$WAIT_STATUS
[[ "$old_status" != 0 ]] || fail "superseded waiter exited successfully"
wait_client "$latest_new"; expect_status 0 "$WAIT_STATUS" latest_new
[[ ! -e "$latest_old_marker" ]] || fail "superseded waiter ran"
wait_for_file "$latest_new_marker"
expect_file_contains "$latest_new_log" newest-ran

announce drop_rejects_work_while_busy
drop_group="drop-$RANDOM"; drop_release="$RUNTIME/drop-release"; drop_started="$RUNTIME/drop-started"; drop_marker="$RUNTIME/drop-ran"
drop_owner_log="$RUNTIME/drop-owner.log"; drop_log="$RUNTIME/drop.log"
start_client "$drop_owner_log" --group "$drop_group" --max-concurrency 1 --queue drop --key drop-owner -- sh -c 'touch "$1"; while ! test -e "$2"; do sleep .01; done; echo owner-end' sh "$drop_started" "$drop_release"; drop_owner=$STARTED_PID
wait_for_file "$drop_started"
run_foreground "$drop_log" --json --group "$drop_group" --max-concurrency 1 --queue drop --key drop-new -- sh -c 'touch "$1"; echo SHOULD-NOT-RUN' sh "$drop_marker"; drop_status=$?
[[ "$drop_status" != 0 ]] || fail "dropped request exited successfully"
[[ ! -e "$drop_marker" ]] || fail "dropped request ran"
expect_file_contains "$drop_log" '"event":"request_dropped"'
touch "$drop_release"
wait_client "$drop_owner"; expect_status 0 "$WAIT_STATUS" drop_owner

announce replace_cancels_active_then_runs_newest
replace_group="replace-$RANDOM"; replace_started="$RUNTIME/replace-started"; replace_old_finished="$RUNTIME/replace-old-finished"
replace_old_log="$RUNTIME/replace-old.log"; replace_new_log="$RUNTIME/replace-new.log"
start_client "$replace_old_log" --group "$replace_group" --queue replace --key replace-key -- sh -c 'touch "$1"; sleep 10; touch "$2"' sh "$replace_started" "$replace_old_finished"; replace_old=$STARTED_PID
wait_for_file "$replace_started"
start_client "$replace_new_log" --group "$replace_group" --queue replace --key replace-key -- sh -c 'echo replacement-ran'; replace_new=$STARTED_PID
wait_client "$replace_old"; old_status=$WAIT_STATUS
[[ "$old_status" != 0 ]] || fail "replaced client exited successfully"
wait_client "$replace_new"; expect_status 0 "$WAIT_STATUS" replacement
[[ ! -e "$replace_old_finished" ]] || fail "replaced process reached its normal completion"
expect_file_contains "$replace_new_log" replacement-ran

announce group_rate_limit_delays_second_start
rate_group="rate-$RANDOM"; rate_one="$RUNTIME/rate-one"; rate_two="$RUNTIME/rate-two"
rate_one_log="$RUNTIME/rate-one.log"; rate_two_log="$RUNTIME/rate-two.log"
start_client "$rate_one_log" --group "$rate_group" --queue fifo --rate-limit 1/600ms --key rate-a -- sh -c 'date +%s%N > "$1"; echo rate-one' sh "$rate_one"; rate_first=$STARTED_PID
wait_for_file "$rate_one"
start_client "$rate_two_log" --group "$rate_group" --queue fifo --rate-limit 1/600ms --key rate-b -- sh -c 'date +%s%N > "$1"; echo rate-two' sh "$rate_two"; rate_second=$STARTED_PID
expect_absent_for "$rate_two" 150
wait_client "$rate_first"; expect_status 0 "$WAIT_STATUS" rate_first
wait_client "$rate_second"; expect_status 0 "$WAIT_STATUS" rate_second
wait_for_file "$rate_two"
rate_delta=$(( $(<"$rate_two") - $(<"$rate_one") ))
(( rate_delta >= 400000000 )) || fail "rate-limited start was only ${rate_delta}ns later"
expect_file_contains "$rate_two_log" rate-two

announce timeout_reports_timeout_outcome
timeout_marker="$RUNTIME/timeout-finished"; timeout_log="$RUNTIME/timeout.log"
run_foreground "$timeout_log" --json --timeout 150ms --kill-after 100ms -- sh -c 'echo started; sleep 10; touch "$1"' sh "$timeout_marker"; timeout_status=$?
expect_status 124 "$timeout_status" timeout
[[ ! -e "$timeout_marker" ]] || fail "timed-out command completed normally"
expect_file_contains "$timeout_log" '"event":"attempt_completed"'
expect_file_contains "$timeout_log" '"outcome":"timed_out"'

announce retries_run_until_success
retry_state="$RUNTIME/retry-count"; retry_log="$RUNTIME/retry.log"
run_foreground "$retry_log" --retries 2 --retry-delay 50ms --backoff fixed -- sh -c 'n=$(cat "$1" 2>/dev/null || echo 0); n=$((n + 1)); echo "$n" > "$1"; echo attempt="$n"; test "$n" -ge 3' sh "$retry_state"; retry_status=$?
expect_status 0 "$retry_status" retry
[[ "$(<"$retry_state")" == 3 ]] || fail "expected 3 attempts, got $(<"$retry_state")"
expect_file_contains "$retry_log" attempt=1
expect_file_contains "$retry_log" attempt=2
expect_file_contains "$retry_log" attempt=3

announce disconnect_does_not_cancel_execution
disconnect_ready="$RUNTIME/disconnect-ready"; disconnect_release="$RUNTIME/disconnect-release"; disconnect_finished="$RUNTIME/disconnect-finished"; disconnect_log="$RUNTIME/disconnect.log"
start_client "$disconnect_log" -- sh -c 'touch "$1"; while ! test -e "$2"; do sleep .01; done; touch "$3"' sh "$disconnect_ready" "$disconnect_release" "$disconnect_finished"; disconnect_client=$STARTED_PID
wait_for_file "$disconnect_ready"
kill -INT "$disconnect_client"
wait_client "$disconnect_client"; expect_status 130 "$WAIT_STATUS" interrupted_client
touch "$disconnect_release"
wait_for_file "$disconnect_finished"

printf '\nPASS: real CLI black-box suite completed successfully\n'
