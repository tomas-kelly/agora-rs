#!/usr/bin/env bash
set -euo pipefail

echo "==> Building agora-rs..."
cargo build --workspace

echo "==> Bootstrapping signing key..."
./target/debug/agora bootstrap

echo ""
echo "Done. Run the swarm with:"
echo "  cargo run -p agora -- start --config agents.local.json"
echo ""
echo "Submit an event:"
echo "  cargo run -p agora -- submit workspace.event.submitted 'Build a REST API for user management'"
