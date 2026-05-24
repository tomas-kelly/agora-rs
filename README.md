# agora-rs

Rust port of the NATS-backed event-driven agent swarm. Each configured agent
runs as a separate process that connects to a local NATS JetStream bus and
reacts to events published by other agents.

## Layout

```
crates/
  agora-core/          # shared library — envelope, bus, daemon runtime, ACP, tokens
  agora/               # supervisor CLI binary
  agora-agent/         # generic config-driven agent process
  agora-console/       # terminal console for sessions, events, agents, history
  daemon-telemetry/    # appends agent.telemetry.logs to .agora/logs/telemetry.jsonl
```

## Prerequisites

- Rust + Cargo (`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`)
- `nats-server` on PATH (`brew install nats-server`)
- `kiro-cli` on PATH and logged in (`kiro-cli login`) — `agents.local.json`
  uses the Kiro ACP backend by default

## Running the swarm

Three steps, in order. Step 0 runs **once per checkout**; steps 1 and 2 are
the everyday loop.

### Step 0 — Bootstrap (required, once)

```bash
./scripts/bootstrap.sh
```

Verify your environment any time with:

```bash
cargo run -p agora -- doctor
```

`doctor` checks `nats-server` and `kiro-cli` on PATH, the signing key, that
the topology validates, and (if the swarm is running) that NATS + JetStream
are healthy. Non-zero exit on any failure, so it's safe to chain into scripts.

This builds the workspace and mints `.kiro/session_token`, the HS256 secret
that signs every actor token on the bus. **Every publish path needs it** —
the supervisor's `submit` CLI, the TUI console, and every `agora-agent`
process. Skipping bootstrap will let `agora run` boot NATS, but the first
idea you submit will fail with a "Signing key not found at .kiro/session_token"
error.

### Step 1 — Start the swarm (Terminal 1, long-running)

```bash
cargo run -p agora -- run agents.local.json
```

Boots NATS + all six Kiro-backed agent processes from the topology. Leave
this terminal running; `Ctrl-C` stops the whole swarm.

### Step 2 — Open the console (Terminal 2)

```bash
cargo run -p agora -- console
```

The TUI window onto the running swarm: sessions / events / agents panes
plus a composer for ideas and direct messages. Safe to quit and reopen
anytime — the JetStream replay catches you up on what you missed.

`cargo run -p agora-console` still works as a compatibility entry point, but
`agora console` is the canonical CLI command.

> The console **requires Step 1 to be running**. If you see
> `cannot connect to nats://127.0.0.1:4222`, that's why — start the
> supervisor first.

### Headless alternative

For scripted use you can skip the console and submit ideas directly:

```bash
cargo run -p agora -- submit "Build a REST API for user management"
```

Or run the bundled smoke-test demo, which submits a canned idea and polls
JetStream until the full loopback completes:

```bash
./scripts/demo.sh                          # canned idea
./scripts/demo.sh "Build a chess engine"   # custom idea
```

The Agora session ID from each event is passed through to ACP so all agent
activity stays attached to the same Agora session.

### Operate the swarm from the CLI

The top-level commands mirror the Docker-style workflow for local operations:

```bash
cargo run -p agora -- ps
cargo run -p agora -- logs backend-coder -f --tail 200
cargo run -p agora -- inspect backend-coder
cargo run -p agora -- events --follow --session-id sess_<id>
cargo run -p agora -- stats
cargo run -p agora -- top backend-coder
cargo run -p agora -- info
cargo run -p agora -- version
```

Object-scoped aliases are available when that reads better:

```bash
cargo run -p agora -- agent ls
cargo run -p agora -- agent logs backend-coder -f
cargo run -p agora -- agent inspect backend-coder --json
cargo run -p agora -- session ls
cargo run -p agora -- session inspect sess_<id>
cargo run -p agora -- system info
cargo run -p agora -- system events --follow
```

### What it looks like

```
 agora console │ nats://127.0.0.1:4222 │ events:24 sessions:2 agents:6 │ Submitted idea to "Task manager"
┌─ Sessions (2) ─────────────┬─ Events ───────────────────────────────────────┬─ Agents (6) ─────────────────┐
│ ▶ Task manager             │ 10:00:01  workspace.idea.submitted   sess_01J… │ ● product-manager            │
│   workspace.idea… · 8 evts │ 10:00:04  product.requirements…      sess_01J… │    :4001 · product           │
│   Chess engine             │ 10:00:09  workspace.design…          sess_01J… │ ● system-architect           │
│   test.passed · 11 evts    │ 10:00:13  code.changed               sess_01J… │    :4002 · architecture      │
│                            │ 10:00:14  code.changed               sess_01J… │ ◐ backend-coder              │
│                            │ 10:00:17  test.failed                sess_01J… │    :4003 · backend, rust     │
│                            │ 10:00:18  security.scan.clean        sess_01J… │ ◐ frontend-coder             │
│                            │ 10:00:21  code.changed               sess_01J… │    :4004 · frontend, tui     │
│                            │ 10:00:24  test.passed                sess_01J… │ ● quality-assurance          │
│                            │                                                │    :4005 · testing           │
│                            │                                                │ ● security-engineer          │
│                            │                                                │    :4006 · security-audit    │
├─ Latest event ─────────────┴────────────────────────────────────────────────┴──────────────────────────────┤
│ topic:   test.passed                                                                                       │
│ event:   evt_01J5ZT9VYC9X7HE8GZ8RVKDM3X                                                                    │
│ session: Task manager  (sess_01J5ZT…)                                                                      │
│ from:    quality-assurance                                                                                 │
│ data:    { "summary": "All tests passing", "passedTests": ["api", "ui_render", "task_crud"] }              │
├─ Compose · active: Task manager ───────────────────────────────────────────────────────────────────────────┤
│ ›  !history backend-coder                                                                                  │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

> Topic colors in a real terminal: `workspace.*` cyan, `code.*` yellow,
> `test.passed` green, `test.failed` red, `security.*` green/red by outcome,
> `human.*` magenta, `agent.*` blue.  Agent status icons: `●` ready,
> `◐` busy, `◌` starting, `○` down.

`!history backend-coder` would replace the **Latest event** pane with a
scrollable conversation timeline — inbound events the agent received,
prompts sent to Kiro, responses received, and outbound events published —
all interleaved by timestamp. PgUp/PgDn or mouse-wheel to scroll, `!page`
to dump into `$PAGER`, `!copy` to drop the whole thing on the clipboard.

### Console keys

**Commands** (type, press Enter)

| Command | Action |
|---|---|
| `!help` | Show the commands panel |
| `!new [name]` | Create a new session (prompts if name omitted), make it active |
| `!rename [name]` | Rename the active session (prompts if name omitted) |
| `!agents` | List agents seen in the active session (with status + event counts) |
| `!status <agent>` | Manifest + per-session activity for that agent |
| `!history <agent>` | Full conversation timeline for that agent in the active session: events received, prompts sent to ACP, responses received, events published |
| `!copy` | Copy command output (or latest event detail) to system clipboard |
| `!editor` | Compose input in `$EDITOR` — drops the TUI, opens your editor with current input, returns when you save and quit |
| `!page` | Open the command output panel in `$PAGER` (e.g. `less`) — useful for long `!history` results |
| `!reset` | Clear the local events / telemetry / sessions view (keeps known agents and session names) |
| `!clear` | Dismiss the command output panel |
| `!exit` / `!quit` | Quit (same as Esc) |

**Submitting work**

| Input | Action |
|---|---|
| Plain text + `Enter` | Submit as `workspace.idea.submitted` (under active session if any, else new) |
| `@agent <msg>` + `Enter` | Direct message to that agent's inbox |
| `/steer @agent <msg>` | Steering message (preempts current work) |
| `/queue @agent <msg>` | Queue behind agent's current event |

**Keys**

| Key | Action |
|---|---|
| `Ctrl-N` / `Ctrl-R` / `Ctrl-X` | Same as `!new` / `!rename` / clear active session |
| `Tab` | If input starts with `@<prefix>`, autocomplete the agent name; otherwise cycle which session is active |
| `Shift-Tab` | Cycle active session backwards |
| `Shift+Enter` / `Alt+Enter` / `Ctrl+J` | Insert a newline (the input box grows) |
| `↑` / `↓` | Scroll events pane by 1 line (pauses auto-scroll) |
| `PgUp` / `PgDn` | Scroll the command output panel, or a long draft in the composer, otherwise events by 5 lines |
| Mouse wheel | Scrolls the composer when the pointer is over a long draft; otherwise scrolls command output or events |
| `End` | Snap back to the live tail |
| `Esc` (in modal) | Cancel naming/renaming |
| `Esc` (normal mode, with command output) | Dismiss the output panel |
| `Esc` / `q` / `Ctrl-C` (otherwise) | Quit |

### Debugging an agent

`!history <agent>` is the main debugging surface — it produces a scrollable
chronological timeline of everything that agent did in the active session:

```
History: backend-coder in "Build user API"
  9 item(s) · PgUp/PgDn or mouse wheel to scroll

10:00:02  ← received  workspace.design.finalized  [evt_01J3K...]
         { "summary": "MVP architecture finalized", ... }

·····  prompt to ACP  ·····
  You are the backend developer. Implement this design: …

·····  response from ACP  ·····
  {"summary":"Implemented initial MVP code changes", ...}

10:00:03  → published code.changed  [evt_01J3K...]
         { "summary": "Implemented initial MVP code changes", ... }
```

Prompts and responses come from per-agent telemetry the `agora-agent`
runtime emits on every event it processes (`agent.telemetry.logs` →
`prompt_sent` / `response_received`). The console subscribes to the
telemetry channel live; restart the console with the swarm running to
catch new flows.

Session names are broadcast as `session.named` events on the bus, so any
other console connected to the same swarm sees them immediately and any
console started later picks them up via the JetStream replay.

The complete loopback cycle fires automatically:

```
workspace.idea.submitted
  → [product-manager]   → product.requirements.defined
    → [system-architect] → workspace.design.finalized
      → [backend-coder]   → code.changed
      → [frontend-coder]  → code.changed
        → [quality-assurance] → test.passed | test.failed
        → [security-engineer] → security.scan.clean | security.alert.found
          → [backend-coder] / [frontend-coder] → code.changed  (repair loop)
```

## Inspect events

```bash
# All events
cargo run -p agora -- events

# Specific session
cargo run -p agora -- events --session-id sess_<id>

# Live stream
cargo run -p agora -- events --follow
```

`agora replay` remains as a compatibility command for the older event dump.

## Architecture

### Overview

```
                            ┌────────────────────────────────────────┐
                            │           NATS JetStream               │
                            │   stream: AGORA_EVENTS                 │
                            │   subjects: workspace.> code.> test.>  │
                            │             security.> human.>         │
                            │             agent.> event.> session.>  │
                            └─┬──────────────────────────────────────┘
                              │
   ┌──────────────┐   publish │   subscribe (durable pull consumer per topic)
   │ agora console│ ──────────┤            ┌────────────────────────────┐
   │ (TUI)        │           ├──────────► │ agora-agent (one per spec) │
   └──────────────┘           │            │   ├─ Mock or Kiro ACP      │
                              │            │   ├─ render prompt         │
   ┌──────────────┐  publish  │            │   ├─ session_prompt(...)   │
   │ agora submit │ ──────────┤            │   ├─ parse response JSON   │
   │ (headless)   │           │            │   └─ publish child event   │
   └──────────────┘           │            └────────────────────────────┘
                              │
                              │ tap            ┌──────────────────┐
                              ├──────────────► │ daemon-telemetry │
                              │                │ → .agora/logs/   │
                              │                │   telemetry.jsonl│
                              │                └──────────────────┘
                              │
   ┌────────────────────────────────────────────────────────────────┐
   │  agora (supervisor)  — spawns nats-server, daemon-telemetry,   │
   │  one agora-agent per topology entry; restarts on crash up to 3 │
   │  attempts before escalating to swarm shutdown.                 │
   └────────────────────────────────────────────────────────────────┘
```

Cascading loopback for a single idea:

```
workspace.idea.submitted
  → [product-manager]   → product.requirements.defined
    → [system-architect] → workspace.design.finalized
      → [backend-coder]   → code.changed
      → [frontend-coder]  → code.changed
        → [quality-assurance] → test.failed   (first attempt)
        → [security-engineer] → security.scan.clean
          → [backend-coder|frontend-coder]    → code.changed (repair)
            → [quality-assurance]             → test.passed
```

All events in one workflow share a `sessionId`; agents resume their ACP
session by that id so the loopback feels like one conversation per agent.

### Wire format (`Envelope`)

Every NATS message is a JSON `Envelope`:

```json
{
  "eventId": "evt_01J...",
  "timestamp": "2026-05-22T10:00:00Z",
  "topic": "workspace.idea.submitted",
  "sender": { "agentName": "agora-cli", "port": 0 },
  "security": { "actorToken": "eyJ..." },
  "context": { "sessionId": "sess_01J...", "affectedFiles": [] },
  "data": { "idea": "Build a REST API" }
}
```

### NATS topology

- **Stream**: `AGORA_EVENTS` — captures `workspace.>`, `code.>`, `test.>`,
  `security.>`, `human.>`, `agent.>`, `event.>`, `session.>`
- **Consumers**: one durable pull consumer per agent per topic —
  `{agent-name}_{topic_with_underscores}` — survives restart
- **KV bucket**: `AGORA_AGENT_REGISTRY` — live agent manifests

### Daemon runtime

`DaemonRunner<A: Agent>` in `agora-core::daemon`:

1. Connects to NATS and creates the JetStream stream
2. Creates one durable pull consumer per declared subscription
3. Spawns a subscriber task for each consumer; messages fan into an `mpsc` channel
4. A single processing loop drains the channel sequentially (one event at a time)
5. Passes a `Publisher` handle to `on_event` so agents can publish child events
6. Emits heartbeats to `AGORA_AGENT_REGISTRY` KV every 5 s

Durable messages are acknowledged after successful agent processing. Failed
agent handling requests JetStream redelivery; invalid envelopes are terminated.

### Security (actor tokens)

Short-lived HS256 JWTs signed with a cluster-local secret (`.kiro/session_token`).
The TUI/CLI mints a token per session. Agent inbound subscriptions enforce the
declared `required_scopes`, and child events are minted with the declared scopes
for their output topic. Verify with `agora_core::tokens::verify_actor_token`.

## Adding a new agent

Most agents are declared in `agents.local.json` and run through the generic
`agora-agent` process. The local topology starts the six workspace Kiro agents
from `.kiro/agents`: `product-manager`, `system-architect`, `backend-coder`,
`frontend-coder`, `security-engineer`, and `quality-assurance`.

1. Add an `agents[]` entry with subscriptions, publish declarations, and prompt templates.
2. Set `acp` or rely on `default_acp` (`kiro` for the checked-in local topology, `mock` for smoke tests).
3. Include every output topic in either `publishes[]` or a subscription `emit`.

For custom Rust behavior, implement `agora_core::daemon::Agent` in a new crate
and call `DaemonRunner::new(daemon, config).run().await` from its `main`.
