# Agora Runtime Guarantees

This document records the runtime behavior Agora currently guarantees and tests.

## Event Storage

- All persisted workflow events are stored in the `AGORA_EVENTS` JetStream
  stream.
- `Bus::publish` sends `Nats-Msg-Id: <eventId>`.
- JetStream duplicate detection is enabled with a 120 second window. Reusing
  the same `eventId` within that window does not store a second event.

## Delivery

- Each agent/topic pair gets one durable pull consumer named with
  `consumer_name(agent, topic)`.
- Durable consumers use explicit acknowledgement.
- First creation uses `DeliverPolicy::New`, so a newly-added agent does not
  replay old backlog by default.
- After creation, the durable consumer resumes from its stored ack floor across
  restarts.
- Work is sequential per agent process: subscription tasks feed a single work
  queue and `on_event` is awaited one item at a time.

## Acknowledgement And Retry

- Events are acknowledged only after `Agent::on_event` returns `Ok(())`.
- If `on_event` returns an error, Agora sends a JetStream NAK with a one second
  redelivery delay.
- Durable consumers use `max_deliver = 5` and `ack_wait = 30s`.
- Invalid envelopes are terminated instead of retried.
- Unauthorized envelopes are dropped and acknowledged, because retrying cannot
  fix an invalid actor token or missing scope.

## Human Input And Tool Approval

- Human input uses `human.interaction.request` and
  `human.interaction.response`.
- Custom Rust agents call `Publisher::ask_human(...)` or
  `Publisher::ask_human_request(...)`; the runtime publishes the request, waits
  by `correlationId`, and resumes the same `on_event` call.
- Config-driven `agora-agent` agents may route output to
  `human.interaction.request`; `agora-agent` waits for the answer and sends it
  back into the same ACP session before continuing.
- ACP `session/request_permission` is bridged to
  `human.interaction.request` with `kind: "tool_approval"`, then mapped back to
  the selected ACP permission option.

## Verified Coverage

The runtime reliability tests cover:

- JetStream duplicate suppression for repeated `eventId` values.
- NAK/redelivery after a transient agent failure.
- Durable consumer retry settings.
- Human input request/response resuming an agent.
- ACP permission requests round-tripping through a fake ACP subprocess.

## Current Gaps

- There is no first-class dead-letter event after max delivery exhaustion yet.
- Retry policy is currently fixed in code, not topology-configurable.
- There is no persisted audit index beyond the event stream itself.
