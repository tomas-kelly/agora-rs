# Agora Agent I/O Contract

This document defines the input and output format for agents that run under
Agora. It is written for users authoring topology files and prompt templates.

Agora has two layers:

- **Event layer:** NATS subjects, JetStream storage, Agora envelopes, actor
  tokens, session ids, and publish authorization.
- **Agent layer:** the prompt sent to an ACP backend and the JSON response
  returned by that backend.

Normal agents should only implement the **agent layer**. The generic
`agora-agent` process owns the event layer: it subscribes to NATS, renders
prompts, parses responses, validates publish permissions, builds envelopes,
mints actor tokens, preserves session context, and publishes output events.

Custom low-level Rust daemons may implement `agora_core::daemon::Agent`
directly. Those daemons are responsible for using the `Publisher` handle
instead of publishing raw events themselves.

## Vocabulary

- **Envelope:** the Agora wire object stored in JetStream.
- **Topic:** a NATS subject such as `workspace.event.submitted` or
  `code.changed`.
- **Session:** a logical workflow identified by `sess_...`.
- **Subscription:** one input topic an agent reacts to.
- **Emit:** the default output event for a subscription.
- **Control fields:** reserved response fields used by `agora-agent`,
  currently `_topic` and `_data`.

## Topology Contract

Each agent is declared in a topology JSON file, usually `agents.local.json`.

```json
{
  "name": "example-agent",
  "port": 4010,
  "capabilities": ["example", "planning"],
  "acp": "stdio",
  "acp_command": "your-acp-client acp --agent example-agent",
  "acp_timeout_secs": 300,
  "publishes": [
    {
      "topic": "example.plan.created",
      "required_scopes": ["workspace:read", "workspace:write"]
    }
  ],
  "subscriptions": [
    {
      "topic": "workspace.event.submitted",
      "required_scopes": ["workspace:read"],
      "prompt_template": "Agora session {{session_id}}.\nInput event: {{topic}}\nData: {{data}}\nReturn exactly one JSON object.",
      "emit": {
        "topic": "example.plan.created",
        "required_scopes": ["workspace:read", "workspace:write"]
      }
    }
  ]
}
```

Rules:

1. `name` must be unique in the topology.
2. `subscriptions[].topic` declares the input events the agent consumes.
3. `subscriptions[].required_scopes` declares the scopes required on inbound
   events.
4. `subscriptions[].prompt_template` renders the prompt sent to ACP.
5. `acp_timeout_secs` can override the topology default for agents with long
   turns; omit it to use `default_acp_timeout_secs`.
6. `subscriptions[].emit.topic` is the default output topic for that input.
7. `publishes[]` declares every topic the agent may publish, including
   branch topics used through `_topic`.
8. Every topic referenced by the topology must be covered by
   `EVENT_STREAM_SUBJECTS` in `agora-core/src/topics.rs`.

## Prompt Input

`agora-agent` renders `prompt_template` with values from the inbound envelope.
Templates currently support these variables:

| Variable | Meaning |
| --- | --- |
| `{{topic}}` | Inbound event topic |
| `{{session_id}}` or `{{sessionId}}` | Agora session id |
| `{{event_id}}` or `{{eventId}}` | Inbound event id |
| `{{data}}` | Inbound event `data` serialized as JSON |
| `{{data.<path>}}` | Nested field lookup inside `data` |

Example:

```text
Agora session {{session_id}}.
You are the planning agent.

Inbound topic: {{topic}}
Text: {{data.text}}

Return exactly one JSON object on the final line.
```

Recommended prompt shape:

```text
Agora session {{session_id}}.
Agent: example-agent
Input event:
- id: {{event_id}}
- topic: {{topic}}
- data: {{data}}

Task:
<agent-specific instructions>

Allowed output topics:
- example.plan.created

Return exactly one JSON object. No markdown fences. No prose after the JSON.
```

## Agent Output

Agents should return exactly one JSON object as their final response. The object
is parsed by `agora-agent`.

For production prompts, prefer a single JSON object with no markdown fences and
no prose after it. During development, `agora-agent` parses responses in this
order:

1. The whole response as JSON.
2. The final non-empty line as JSON.
3. The last parseable fenced JSON block.
4. Plain text wrapped as `{ "summary": "<response text>" }`.

### Single-route Output

If the subscription has an `emit.topic`, the agent can return payload data
directly:

```json
{
  "status": "succeeded",
  "summary": "Created an implementation plan.",
  "details": {
    "steps": ["Define API", "Implement handler", "Add tests"]
  },
  "followUps": []
}
```

`agora-agent` publishes this object as the `data` for the configured
`emit.topic`.

### Branching Output

If the agent needs to choose among multiple allowed output topics, return
`_topic` and `_data`:

```json
{
  "_topic": "test.failed",
  "_data": {
    "status": "failed",
    "summary": "cargo test failed in agora-console",
    "reason": "ui::tests::layout_snapshot changed",
    "reproSteps": ["cargo test -p agora-console"]
  }
}
```

Routing rules:

1. If `_topic` is present, publish to `_topic`.
2. Else publish to the subscription's `emit.topic`.
3. If neither `_topic` nor `emit.topic` exists, publish no event.
4. If `_data` is present, use `_data` as the event payload.
5. Else use the full response object after removing control fields.
6. `_topic` must be declared in `publishes[]`.

`Publisher::publish` enforces this at runtime: undeclared output topics are
rejected and the event is not published.

### Plain Text Output

If the ACP response is not valid JSON, `agora-agent` wraps it as:

```json
{
  "summary": "<response text>"
}
```

This fallback is useful for development, but production agents should return
valid JSON.

## Common Payload Fields

Use these fields consistently when they apply:

```json
{
  "status": "succeeded",
  "summary": "One-line human-readable summary.",
  "details": {},
  "artifacts": [],
  "changedFiles": [],
  "testsRun": [],
  "risks": [],
  "followUps": []
}
```

Allowed `status` values:

- `succeeded`
- `failed`
- `blocked`
- `needs_input`
- `no_op`

`summary` should always be present. Other fields are topic-specific.

For `code.changed`, `changedFiles` is also copied into the envelope context as
`affectedFiles`. A `code.changed` event must include at least one non-empty file
path in `changedFiles` (or `affectedFiles`) or the runtime rejects the publish.

## Human Interaction

Agents that need operator input should publish `human.interaction.request`
through the runtime API, not by inventing an ad hoc topic.

For agents using `agora-agent`, this means returning an output that routes to
an allowed human-interaction topic. The runner publishes the request, waits for
a matching `human.interaction.response`, and sends the answer back into the
same ACP session so the agent can continue and produce its final event. Custom
Rust agents can call `Publisher::ask_human(...)`, which uses the same request /
response event pair.

ACP tool approvals use the same event pair. When an ACP backend calls
`session/request_permission`, `agora-agent` publishes a
`human.interaction.request` with `kind: "tool_approval"` and structured
`details.toolCall` / `details.options`, waits for the operator response, and
then returns the selected ACP permission option. To allow this path to run, do
not launch ACP agents with a blanket trust flag such as `--trust-all-tools`.

Tool approval response answers should match an ACP `optionId` such as
`allow-once` or `reject-once`. The runtime also accepts short operator answers
such as `allow`, `approve`, `reject`, or `deny` and maps them to the matching
ACP option when available.

## No-Op Events

Agents that receive an event but have no in-scope work should publish
`agent.noop` only if the topology declares it in `publishes[]`.

Use this when an event is intentionally ignored, for example a frontend coder
receiving a design with backend-only tasks. Do not publish `code.changed` with
an empty `changedFiles` array; `code.changed` requires at least one real changed
file so QA and security are not asked to review empty work.

Recommended payload:

```json
{
  "status": "no_op",
  "summary": "No frontend work required.",
  "reason": "The design contains no frontend tasks.",
  "checkedScope": ["frontendTasks"]
}
```

## Direct Messages

Every `agora-agent` automatically subscribes to:

```text
agent.inbox.<agent-name>
```

The payload shape is:

```json
{
  "messageType": "steering",
  "recipient": "example-agent",
  "sessionId": "sess_...",
  "message": "Focus on the failing test first."
}
```

Direct messages must always be scoped to a concrete session id. The session id
is present both in the envelope context and in `data.sessionId`.

Supported message types today are `steering` and `queue`. The exact scheduling
semantics are runtime behavior; agents should treat the message as additional
instructions in the current session.

## What Agents Must Not Do

Normal agents must not:

- Publish directly to NATS.
- Mint actor tokens.
- Create Agora envelopes by hand.
- Change the session id.
- Publish topics not declared in `publishes[]`.
- Depend on another agent's hardcoded name unless the topology/prompt makes
  that relationship explicit.
- Return multiple final JSON objects.
- Hide event-routing decisions in prose.

## What Agora Guarantees

For agents using `agora-agent`, Agora handles:

- Durable NATS subscription setup.
- Prompt rendering.
- ACP session creation and reuse keyed by Agora session id.
- Prompt and response telemetry.
- JSON parsing of the ACP response.
- Default route selection from `emit.topic`.
- Optional branch route selection from `_topic`.
- Envelope creation.
- Actor token minting.
- Session id inheritance.
- JetStream publish.

## Example End-to-End Flow

Topology:

```json
{
  "name": "planner",
  "port": 4010,
  "capabilities": ["planning"],
  "publishes": [
    { "topic": "plan.created", "required_scopes": ["workspace:write"] }
  ],
  "subscriptions": [
    {
      "topic": "workspace.event.submitted",
      "required_scopes": ["workspace:read"],
      "prompt_template": "Session {{session_id}}\nText: {{data.text}}\nReturn JSON.",
      "emit": { "topic": "plan.created", "required_scopes": ["workspace:write"] }
    }
  ]
}
```

Inbound event data:

```json
{
  "text": "Build a small issue tracker"
}
```

Agent response:

```json
{
  "status": "succeeded",
  "summary": "Planned a small issue tracker.",
  "details": {
    "entities": ["issue", "user", "comment"],
    "nextSteps": ["Define schema", "Implement CRUD", "Add tests"]
  }
}
```

Published event:

```json
{
  "topic": "plan.created",
  "context": {
    "sessionId": "sess_..."
  },
  "data": {
    "status": "succeeded",
    "summary": "Planned a small issue tracker.",
    "details": {
      "entities": ["issue", "user", "comment"],
      "nextSteps": ["Define schema", "Implement CRUD", "Add tests"]
    }
  }
}
```

## Production Checklist

Before relying on a topology in production:

- Every agent has a unique `name`.
- Every input topic is declared in `subscriptions[]`.
- Every output topic is declared in `publishes[]` or `emit`.
- Every output topic is covered by `EVENT_STREAM_SUBJECTS`.
- Prompts list the allowed output topics.
- Prompts require exactly one final JSON object.
- Branching agents document their allowed `_topic` values.
- No-op branches use `agent.noop`; they do not publish empty `code.changed`
  events.
- Downstream agents document the fields they expect in `data`.
- Tests or smoke checks validate the happy path and at least one failure path.
