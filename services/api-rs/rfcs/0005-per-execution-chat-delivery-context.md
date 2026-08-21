# Per-execution chat delivery context

Status: Proposed

## Problem

A durable Centaur session has one stable logical chat destination. On Discord,
an inline-reply conversation deliberately reuses that session across multiple
messages. The physical reply target, however, changes on every execution.
Transport renderers already receive the current delivery key, but sandbox tools
resolve the session's original destination through the scoped-context API. A
file upload in a later turn therefore replies to the conversation root instead
of the message that triggered the active execution.

## Decision

Keep the session destination immutable. For the authenticated scoped-context
endpoint only, overlay a Discord session's `reply_to_message_id` with the
`source.message_id` from the currently queued or running execution's trusted
invocation. The API validates that invocation against the session's platform,
guild, channel, and thread before enqueueing it. Free-form execution metadata
may not supply or retain the reserved `invocation` field.

The ordinary read-only session-context endpoint continues to report the stable
session destination. Other chat platforms are unchanged. If no active trusted
Discord invocation exists, scoped context falls back to the session destination.

## Benefits

- Uploads and other scoped Discord delivery tools reply to the triggering turn.
- Logical session identity, replay, serialization, and renderer delivery keys
  remain unchanged.
- The generic fix applies to every Centaur Discord application without adding
  product concepts to the control plane.

## Costs and compatibility

The scoped-context response becomes execution-sensitive while work is active.
Clients that assumed its Discord reply ID was immutable will now see the current
turn, which is the intended mutation target. One active-execution lookup is
added to scoped-context requests. Completed and idle sessions retain the old
fallback behavior.

## Security

The override is never accepted as a destination argument from a sandbox tool.
It comes from ingress-authenticated invocation data, is checked against the
session destination before persistence, and may change only the reply message
within the already-authorized Discord conversation. The subject-bearing
sandbox principal must exactly own the addressed session, so changing
`CENTAUR_THREAD_KEY` cannot access another session. Caller-controlled metadata
is stripped of the reserved invocation key so it cannot forge the override.

## Alternatives

1. Mutate the session destination on every turn. Rejected because it makes
   durable session identity race with execution and recovery.
2. Pass arbitrary destinations to upload tools. Rejected because it expands
   sandbox authority to the bot token's reach.
3. Encode the current message in the logical thread key. Rejected because it
   creates a new session per reply and loses inline conversation continuity.
4. Add a general artifact-to-renderer protocol. Deferred because it changes the
   trusted response contract, sanitization, retry, and composition semantics.
5. Keep root-targeted uploads. Acceptable only if per-turn reply fidelity is not
   required; kill this change if trusted invocation provenance cannot be
   maintained.
