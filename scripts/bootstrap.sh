#!/usr/bin/env bash
set -euo pipefail

echo "==> Building agora-rs..."
cargo build --workspace

echo "==> Bootstrapping signing key..."
./target/debug/agora bootstrap

echo ""
echo "Done. Run the swarm with:"
echo "  cargo run -p agora -- run agents.local.json"
echo ""
echo "Submit an idea:"
echo "  cargo run -p agora -- submit 'Build a REST API for user management'"
