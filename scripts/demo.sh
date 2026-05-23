#!/usr/bin/env bash
# Submit a canned idea to a running swarm and wait for the loopback to
# complete. Useful both as a quick smoke test and as something to show to
# someone who's never seen the swarm before.
#
# Usage:
#   ./scripts/demo.sh                          # canned idea
#   ./scripts/demo.sh "Build a chess engine"   # custom idea
set -euo pipefail

cd "$(dirname "$0")/.."

# Prefer the prebuilt binary if it exists; fall back to `cargo run` so the
# script still works on a fresh clone.
if [[ -x "./target/debug/agora" ]]; then
  AGORA=("./target/debug/agora")
elif [[ -x "./target/release/agora" ]]; then
  AGORA=("./target/release/agora")
else
  echo ">>> No built binary; using 'cargo run' (slower) ..."
  AGORA=("cargo" "run" "-q" "-p" "agora" "--")
fi

run_agora() { "${AGORA[@]}" "$@"; }

# Preflight
echo ">>> agora doctor"
if ! run_agora doctor; then
  echo
  echo "Doctor reported failures. Fix them, then re-run this script."
  exit 1
fi

IDEA="${1:-Build a small task manager with a REST API and a single-page UI}"
echo
echo ">>> Submitting idea:"
echo "    \"$IDEA\""
echo

OUTPUT="$(run_agora submit "$IDEA")"
echo "$OUTPUT"
SESSION_ID="$(echo "$OUTPUT" | grep -oE 'sess_[A-Za-z0-9]+' | head -n1)"
if [[ -z "${SESSION_ID:-}" ]]; then
  echo "Could not parse session id from submit output."
  exit 1
fi

echo
echo ">>> Watching $SESSION_ID for test.passed (timeout: 120s)"
echo "    Tail the swarm logs in another shell with:"
echo "      tail -f .agora/logs/*.log"
echo

deadline=$(( $(date +%s) + 120 ))
while (( $(date +%s) < deadline )); do
  if run_agora replay --session-id "$SESSION_ID" 2>/dev/null | grep -q '\btest\.passed\b'; then
    echo
    echo ">>> Loopback completed."
    echo
    run_agora replay --session-id "$SESSION_ID"
    exit 0
  fi
  printf '.'
  sleep 3
done

echo
echo ">>> Timed out. Current session state:"
run_agora replay --session-id "$SESSION_ID"
exit 1
