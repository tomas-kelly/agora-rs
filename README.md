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
- The ACP-compatible stdio client configured in `agents.local.json`, currently
  `kiro-cli acp`, available on PATH and logged in.

## Running the swarm

Three steps, in order. Step 0 runs **once per checkout**; steps 1 and 2 are
the everyday loop.

### Step 0 — Bootstrap (recommended, once)

```bash
./scripts/bootstrap.sh
```

Verify your environment any time with:

```bash
cargo run -p agora -- doctor
```

`doctor` checks `nats-server`, each configured ACP command, the signing key,
that the topology validates, and (if the swarm is running) that NATS,
JetStream, durable consumers, and the agent registry are healthy. Non-zero
exit on any failure, so it's safe to chain into scripts.

This builds the workspace and mints `.agora/session_token`, the HS256 secret
that signs every actor token on the bus. **Every publish path needs it** —
the `submit` CLI, the TUI console, and every `agora-agent` process. `agora start`
also creates the default key if it is missing, but running bootstrap up front
keeps `doctor`, `submit`, and `console` happy before the supervisor starts.

### Step 1 — Start the swarm (Terminal 1, long-running)

```bash
cargo run -p agora -- start --config agents.local.json
```

Boots NATS + all six ACP-backed agent processes from the topology. Leave
this terminal running; `Ctrl-C` stops the whole swarm.

To run it in the background:

```bash
cargo run -p agora -- start --config agents.local.json --detach
cargo run -p agora -- stop
```

Detached runtimes are identified by the topology's `pidDir`, especially
`.agora/pids/agora.pid` by default. `agora stop` targets `agents.local.json`;
use `agora stop --config path/to/topology.json` when you started a different
topology. `agora run <config>` remains as a compatibility command for the old
startup syntax.

### Step 2 — Open the console (Terminal 2)

```bash
cargo run -p agora -- console
```

The TUI window onto the running swarm: sessions / events / agents panes
plus a composer for events and direct messages. Safe to quit and reopen
anytime — the JetStream replay catches you up on what you missed.
Composer history is saved to `.agora/console_history.json`; use
`--history-path <path>` to move it or `--history-path ""` to disable disk
history for that run.

`cargo run -p agora-console` still works as a compatibility entry point, but
`agora console` is the canonical CLI command.

> The console **requires Step 1 to be running**. If you see
> `cannot connect to nats://127.0.0.1:4222`, that's why — start the
> supervisor first.

### Headless alternative

For scripted use you can skip the console and submit events directly:

```bash
cargo run -p agora -- submit workspace.event.submitted "Build a REST API for user management"
```

Or run the bundled smoke-test demo, which submits a canned event and polls
JetStream until the full loopback completes:

```bash
./scripts/demo.sh                          # canned event
./scripts/demo.sh "Build a chess engine"   # custom event text
```

The Agora session ID from each event is passed through to ACP so all agent
activity stays attached to the same Agora session.

## CLI reference

The top-level commands mirror the Docker-style workflow for local operations:

```bash
cargo run -p agora -- ps
cargo run -p agora -- logs backend-coder -f --tail 200
cargo run -p agora -- logs "Task manager" --tail 200
cargo run -p agora -- inspect backend-coder
cargo run -p agora -- events sess_<id> --follow
cargo run -p agora -- stats
cargo run -p agora -- top backend-coder
cargo run -p agora -- info
cargo run -p agora -- version
```

Core commands:

| Command | Use |
|---|---|
| `agora start --config agents.local.json` | Start NATS, telemetry, and all agents from the topology. Add `--detach` to run in the background. |
| `agora console` | Open the TUI against the running swarm. |
| `agora submit <topic> <data>` | Publish an event without opening the TUI. Plain text becomes `{ "text": "..." }`; valid JSON is sent as-is. |
| `agora ps` | Show supervisor, service, and agent process status from pid files. |
| `agora logs [target]` | List log targets, or tail one target. |
| `agora events` | Dump or follow the `AGORA_EVENTS` stream. |
| `agora inspect <target>` | Inspect an agent, session id, service/process, or runtime object. |
| `agora stats [target]` | Show local process CPU/memory for the runtime or one target. |
| `agora top <target>` | Show the local process tree for an agent or service. |
| `agora info` | Summarize topology, bus, stream, registry, sessions, and process state. |
| `agora stop [target]` | Stop the whole runtime or one managed process. |
| `agora restart <agent>` | Restart one agent while `agora start` owns the swarm. |
| `agora status <agent>` | Print one agent manifest and optional session activity. |
| `agora history <agent> --sessionId <id>` | Show one agent's event and telemetry timeline in a session. |
| `agora message --agent <agent> --sessionId <id> <text...>` | Send a session-scoped steering or queue message to an agent inbox. |
| `agora bootstrap` | Mint the local signing key at `.agora/session_token`. |
| `agora doctor` | Run environment, topology, key, process, bus, and registry checks. |
| `agora version` | Print CLI version metadata. |

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

### Logs

Log targets are derived from the topology. Run `logs` without a target to list
them:

```bash
cargo run -p agora -- logs
cargo run -p agora -- logs backend-coder --tail 200
cargo run -p agora -- logs backend-coder --follow
cargo run -p agora -- logs backend-coder --since 10m --timestamps
cargo run -p agora -- logs telemetry-jsonl --tail 50
cargo run -p agora -- logs "Task manager" --tail 50 --follow
```

Targets include `agora`, `nats`, `daemon-telemetry`, every configured agent
name, and `telemetry-jsonl`. `--lines` is accepted as an alias for `--tail`.
`--since` accepts RFC3339 timestamps or durations like `10m`, `2h`, or `1d`,
and applies to log lines with parseable timestamps. If the target is not a
known process log target, `logs` treats it as an event target and prints the
matching session/event stream from JetStream. Use `--bus-url` to override the
topology bus when tailing events.

### Events and sessions

`events` reads the JetStream backlog and can also subscribe live:

```bash
cargo run -p agora -- events
cargo run -p agora -- events sess_<id>
cargo run -p agora -- events "Task manager"
cargo run -p agora -- events --sessionId sess_<id>
cargo run -p agora -- events --agent backend-coder
cargo run -p agora -- events --topic 'code.>'
cargo run -p agora -- events 'code.>' --tail 20
cargo run -p agora -- events --json
cargo run -p agora -- events --follow
```

The optional positional target resolves to a session id, session name, agent
name, or topic pattern. `--tail` limits backlog output before follow mode
begins, and `--since` accepts the same RFC3339/duration forms as `logs`.

Session helpers operate on the same event stream:

```bash
cargo run -p agora -- sessions
cargo run -p agora -- session ls
cargo run -p agora -- session new "Task manager"
cargo run -p agora -- session rename sess_<id> "Task manager"
cargo run -p agora -- session delete sess_<id>
cargo run -p agora -- session ls --include-deleted
cargo run -p agora -- session inspect sess_<id> --json
cargo run -p agora -- session history sess_<id>
```

`session delete` publishes a `session.deleted` tombstone. It hides the session
from default session lists and the console, but keeps the event history
available for `session history`, `session inspect`, and audit workflows.

`agora replay` remains as a compatibility command for the older event dump.

### Agents and runtime

Agent helpers are aliases over the top-level commands, scoped to one agent:

```bash
cargo run -p agora -- agents
cargo run -p agora -- agent ls
cargo run -p agora -- agent inspect backend-coder --json
cargo run -p agora -- agent status backend-coder --sessionId sess_<id>
cargo run -p agora -- agent history backend-coder --sessionId sess_<id>
cargo run -p agora -- agent message --agent backend-coder --sessionId sess_<id> "focus on the failing test"
cargo run -p agora -- agent message --agent backend-coder --sessionId sess_<id> --message-type queue "queue this after current work"
cargo run -p agora -- agent restart backend-coder
cargo run -p agora -- agent stop backend-coder
```

Runtime helpers work from `.agora/pids`, `.agora/logs`, the topology, and the
NATS registry:

```bash
cargo run -p agora -- info --json
cargo run -p agora -- inspect runtime --json
cargo run -p agora -- inspect nats
cargo run -p agora -- stats backend-coder
cargo run -p agora -- top backend-coder
cargo run -p agora -- system ps
cargo run -p agora -- system stats
```

### What it looks like

```
 agora console │ nats://127.0.0.1:4222 │ events:24 sessions:2 agents:6 │ Submitted event to "Task manager"
┌─ Sessions (2) ─────────────┬─ Events: Task manager (8/8) ───────┬─ Event details (8/8) ────────────────────┐
│ ▶ Task manager             │ 10:00:01  workspace.event.submitted│ {                                        │
│   Chess engine             │ 10:00:04  product.requirements...  │   "topic": "test.passed",               │
│                            │ 10:00:09  workspace.design...      │   "eventId": "evt_01J5ZT9VYC9X7...",    │
│                            │ 10:00:13  code.changed             │   "timestamp": "2026-05-24T10:00:24Z",  │
├─ Agents (6) ───────────────┤ 10:00:14  code.changed             │   "sender": {                           │
│ ● product-manager          │ 10:00:17  test.failed              │     "agentName": "quality-assurance"    │
│    :4001 · product         │ 10:00:18  security.scan.clean      │   },                                    │
│ ● system-architect         │›10:00:24  test.passed              │   "data": { "summary": "All tests..." } │
│    :4002 · architecture    │                                    │                                          │
│ ◐ backend-coder            │                                    │                                          │
│    :4003 · backend, rust   │                                    │                                          │
└────────────────────────────┴────────────────────────────────────┴──────────────────────────────────────────┘
┌─ Compose · active: Task manager ───────────────────────────────────────────────────────────────────────────┐
│ ›  Build a collaborative editor with offline support, conflict resolution,                                 │
│    audit logging, and a minimal admin view.                                                                │
│                                                                                                            │
│    Prioritize the event model, storage boundaries, and the first regression                                │
│    tests we should run.                                                                                   │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

> Topic colors in a real terminal: `workspace.*` cyan, `code.*` yellow,
> `test.passed` green, `test.failed` red, `security.*` green/red by outcome,
> `human.*` magenta, `agent.*` blue. Agent status icons: `●` ready,
> `◐` busy, `◌` starting/stale, `◑` draining, `○` down.

The composer is intentionally large enough for multi-line prompts. Long drafts
scroll with PgUp/PgDn or with the mouse wheel over the composer. The Events pane
shows only events from the active session and acts like a stream browser:
`↑` / `↓` select an event without opening details. Press `Enter` with an empty
composer to open the full Event Details JSON to the right of the Events pane,
and press `Esc` to close it. `End` returns to the live tail and closes Event
Details. Agents sit below Sessions in the left sidebar.
`Tab` and `Shift-Tab` move focus between visible panes and the composer. With
Agents focused, `↑` / `↓` select an agent, `Enter` opens its live tail for the
active session, `h` opens history, `s` opens status, and `m` starts a direct
message to that agent. `M` starts a queued message instead.

Pending human input and ACP tool approvals are surfaced as
`human.interaction.request` events. The status bar shows the pending count,
Sessions and Agents display `?N` badges, and the matching event row is marked
with `?`. Select the request and press Enter to open it; the composer switches
to response mode and sends `human.interaction.response` back to the same
session. Config-driven agents resume their same ACP session after the response,
then continue toward their final output event. `!pending` lists all unresolved
requests and `!pending next` jumps to the first one.

`!tail backend-coder` opens a dedicated Agent Output pane for the active
session. It shows the prompt sent to ACP and streams ACP response chunks as
they arrive, before the final output event is published. `!history
backend-coder` shows the same conversation timeline as a static snapshot —
inbound events the agent received, prompts sent to ACP, responses received, and
outbound events published — all interleaved by timestamp. PgUp/PgDn scroll the
focused output pane, `!page` dumps command output or Agent Output into
`$PAGER`, and `!copy` puts it on the clipboard.

### Console keys

**Commands** (type, press Enter)

| Command | Action |
|---|---|
| `!help` | Show the commands panel |
| `!new [name]` | Create a new session (prompts if name omitted), make it active |
| `!rename [name]` | Rename the active session (prompts if name omitted) |
| `!delete [session]` | Hide the active session, or a named/id session, without erasing history |
| `!agents` | List agents seen in the active session (with status + event counts) |
| `!status <agent>` | Manifest + per-session activity for that agent |
| `!history <agent>` | Full conversation timeline for that agent in the active session: events received, prompts sent to ACP, responses received, events published |
| `!tail <agent>` | Live ACP output for that agent in the active session; updates while the agent is still working |
| `!pending [next]` | List pending human input/tool approvals, or jump to the first pending request |
| `!panel <name>` | Toggle `sessions`, `events`, `agents`, `detail`, or `all` panels |
| `!copy` | Copy command output (or selected event detail) to system clipboard |
| `!editor` | Compose input in `$EDITOR` — drops the TUI, opens your editor with current input, returns when you save and quit |
| `!page` | Open the command output panel in `$PAGER` (e.g. `less`) — useful for long `!history` results |
| `!clear` | Clear local events and telemetry from the console view; keep sessions, session names, active session, and known agents |
| `!exit` / `!quit` | Quit (same as Esc) |

`Ctrl-K` opens the command palette. While typing a command, topic, session
name, tag, bookmark label, or agent reference, fuzzy completion suggestions
appear above the composer. Press `↑` / `↓` to choose a suggestion and `Enter`
to accept it; `Tab` still accepts a unique match or extends to the longest
common prefix. When the composer is empty, `Tab` cycles pane focus instead.
Composer history persists in `.agora/console_history.json` by default; pass
`--history-path <path>` to move it or `--history-path ""` to keep it in memory
only.

**Submitting work**

| Input | Action |
|---|---|
| Plain text + `Enter` | Submit as the console's configured submit topic, `workspace.event.submitted` by default |
| `!submit <topic> <data>` | Publish a one-off event from the console; data may be JSON or text |
| `@agent <msg>` + `Enter` | Direct message to that agent's inbox in the active session |
| `/steer @agent <msg>` | Steering message in the active session |
| `/queue @agent <msg>` | Queue behind agent's current event in the active session |

**Keys**

| Key | Action |
|---|---|
| `Ctrl-N` / `Ctrl-R` / `Ctrl-X` | Same as `!new` / `!rename` / clear active session |
| `F1` / `F2` / `F3` / `F4` | Toggle sessions / events / agents / detail panels |
| `Tab` | Complete command/topic/session/agent input; with an empty composer, focus the next visible pane |
| `Shift-Tab` | Focus the previous visible pane |
| `Ctrl-K` | Open the command palette |
| `Ctrl-P` / `Alt+↑` | Previous composer entry from anywhere |
| `Alt+↓` | Next composer entry from anywhere |
| `Shift+Enter` / `Alt+Enter` / `Ctrl+J` | Insert a newline (the input box grows) |
| `↑` / `↓` with completions open | Select previous / next completion |
| `↑` / `↓` with Composer focused | Previous / next composer history entry |
| `↑` / `↓` with Sessions focused | Switch the active session |
| `↑` / `↓` with Events focused | Select the previous / next event in the stream |
| `↑` / `↓` with Agents focused | Select the previous / next agent |
| `↑` / `↓` with Agent Output focused | Scroll the live agent output |
| `Enter` with Events focused and empty composer | Open full Event Details for the selected event |
| `Enter` with Agents focused and empty composer | Open live `!tail` for the selected agent |
| `h` / `s` / `m` / `M` with Agents focused | Open selected-agent history / status / direct-message draft / queued-message draft |
| `PgUp` / `PgDn` | Scroll Agent Output, command output, full event details, or a long draft; otherwise select events by 5 lines |
| Mouse wheel | Scrolls the composer when the pointer is over a long draft; otherwise scrolls command output, Event Details, or selects events |
| `End` | Snap back to the live tail |
| `Esc` (in modal) | Cancel naming/renaming |
| `Esc` (normal mode, with palette/output/details) | Dismiss completions, command output, Agent Output, or Event Details |
| `Esc` / `q` / `Ctrl-C` (otherwise) | Quit |

### Debugging an agent

`!tail <agent>` is the live debugging surface — it produces a scrollable
chronological timeline of everything that agent is doing in the active session,
including streaming ACP response chunks before an event is emitted. Use
`!history <agent>` when you want a frozen snapshot of the same view:

```
History: backend-coder in "Build user API"
  9 item(s) · PgUp/PgDn or mouse wheel to scroll

10:00:02  ← received  workspace.design.finalized  [evt_01J3K...]
         { "summary": "MVP architecture finalized", ... }

·····  prompt to ACP  ·····
  You are the backend developer. Implement this design: …

·····  response from ACP (streaming)  ·····
  {"summary":"Implemented initial MVP code changes", ...}

10:00:03  → published code.changed  [evt_01J3K...]
         { "summary": "Implemented initial MVP code changes", ... }
```

Prompts and responses come from per-agent telemetry the `agora-agent`
runtime emits on every event it processes (`agent.telemetry.logs` →
`prompt_sent` / `response_chunk` / `response_received`). The console subscribes
to the telemetry channel live; restart the console with the swarm running to
replay prior flows from JetStream.

Session metadata is event sourced. Names are broadcast as `session.named`;
deletions are broadcast as `session.deleted` tombstones, so any other console
connected to the same swarm sees the change immediately and any console started
later picks it up via the JetStream replay.

The loopback cycle is event driven. QA and security both react to
`code.changed`; repair work is triggered only when QA publishes `test.failed`
or security publishes `security.alert.found`:

```
workspace.event.submitted
  → [product-manager]   → product.requirements.defined
    → [system-architect] → workspace.design.finalized
      → [backend-coder]   → code.changed
      → [frontend-coder]  → code.changed
        → [quality-assurance] → test.passed | test.failed
        → [security-engineer] → security.scan.clean | security.alert.found
          → [backend-coder] / [frontend-coder] → code.changed  (repair loop)
```

Each configured ACP agent has a workflow playbook in `agents/*.md`.
Those files define the events the agent consumes, realistic operating steps,
branching outputs such as `human.interaction.request` or `agent.noop`, and the
final JSON shape expected by `agora-agent`.

Runtime delivery and retry semantics are documented in
[`docs/runtime-guarantees.md`](docs/runtime-guarantees.md).

## Troubleshooting

Start with:

```bash
cargo run -p agora -- doctor
cargo run -p agora -- info
cargo run -p agora -- ps
```

If the console says it cannot connect to NATS, make sure `agora start --config
agents.local.json` is still running. `agora ps` should show the supervisor,
`nats`, `daemon-telemetry`, and each agent as `running`.

If agents show as `stale` or `down`, inspect the process and registry state:

```bash
cargo run -p agora -- ps
cargo run -p agora -- agents
cargo run -p agora -- inspect backend-coder
cargo run -p agora -- logs backend-coder --tail 200
```

Stale pid files are removed by stopping the affected target or the whole
runtime:

```bash
cargo run -p agora -- stop backend-coder
cargo run -p agora -- stop
```

If old Python publishers or legacy processes are still writing registry keys,
stop them before pruning:

```bash
cargo run -p agora -- stop --legacy
cargo run -p agora -- registry prune
cargo run -p agora -- registry prune --apply
```

`registry prune` is a dry run by default. `--apply` purges stale, malformed, or
out-of-topology keys from `AGORA_AGENT_REGISTRY`.

Runtime files live under `.agora/`:

| Path | Contents |
|---|---|
| `.agora/pids/` | Supervisor, service, and agent pid files used by `ps`, `stop`, and `restart`. |
| `.agora/logs/*.log` | Process logs for the supervisor, NATS, telemetry, and each agent. |
| `.agora/logs/telemetry.jsonl` | Raw agent telemetry, including prompts and ACP responses. |
| `.agora/nats/` | Persistent NATS JetStream store for `AGORA_EVENTS`, consumers, and registry KV data. |

If submit or console publish paths fail with a missing key error, rerun:

```bash
cargo run -p agora -- bootstrap
```

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
   └──────────────┘           │            │   ├─ Mock or stdio ACP    │
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

Cascading loopback for a single submitted event:

```
workspace.event.submitted
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
  "topic": "workspace.event.submitted",
  "sender": { "agentName": "agora-cli", "port": 0 },
  "security": { "actorToken": "eyJ..." },
  "context": { "sessionId": "sess_01J...", "affectedFiles": [] },
  "data": { "text": "Build a REST API" }
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

Short-lived HS256 JWTs signed with a cluster-local secret (`.agora/session_token`).
The TUI/CLI mints a token per session. Agent inbound subscriptions enforce the
declared `required_scopes`, and child events are minted with the declared scopes
for their output topic. Verify with `agora_core::tokens::verify_actor_token`.

## Adding a new agent

Most agents are declared in `agents.local.json` and run through the generic
`agora-agent` process. The local topology starts the six workspace ACP agents
from `agents`: `product-manager`, `system-architect`, `backend-coder`,
`frontend-coder`, `security-engineer`, and `quality-assurance`.

Agents should follow the prompt/response contract in
[`docs/agent-io-contract.md`](docs/agent-io-contract.md). In short:
topology declares input topics, allowed output topics, and prompt templates;
agents return exactly one JSON object; `agora-agent` validates, envelopes, and
publishes the resulting event.

1. Add an `agents[]` entry with subscriptions, publish declarations, and prompt templates.
2. Set `acp` or rely on `default_acp` (`stdio` for the checked-in local topology, `mock` for smoke tests).
3. Tune ACP call duration with topology `default_acp_timeout_secs` or per-agent `acp_timeout_secs` when a client needs longer turns.
4. Include every output topic in either `publishes[]` or a subscription `emit`.

For custom Rust behavior, implement `agora_core::daemon::Agent` in a new crate
and call `DaemonRunner::new(daemon, config).run().await` from its `main`.
