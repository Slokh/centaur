import type { DiscordbotFetch, DiscordbotOptions } from "./types";
import type { StateAdapter } from "chat";

const INGESTION_INDEX_KEY = "discordbot:application-ingestion:index";
const INGESTION_EVENT_PREFIX = "discordbot:application-ingestion:event:";
const INGESTION_RETENTION_MS = 30 * 24 * 60 * 60 * 1_000;

export type ObservedDiscordMessage = {
  guildId: string;
  channelId: string;
  threadId?: string;
  messageId: string;
  authorId: string;
  authorName?: string;
  displayName?: string;
  authorIsBot?: boolean;
  replyToMessageId?: string;
  normalizedPayload?: unknown;
  content: string;
  createdAt: string;
  editedAt?: string;
  attachments: Array<{
    id: string;
    filename: string;
    contentType?: string;
    size: number;
    url: string;
  }>;
};

/**
 * Forward a normalized Discord message to a configured private application.
 * Source keys make retries harmless. A later reconciliation job remains the
 * authority for gaps caused by a prolonged application outage.
 */
export async function ingestObservedDiscordMessage(
  options: DiscordbotOptions,
  message: ObservedDiscordMessage,
  state?: StateAdapter,
): Promise<void> {
  if (!options.applicationIngestionUrl || !options.applicationIngestionToken) {
    return;
  }
  for (const event of messageIngestionEvents(message)) {
    await persistAndPostIngestionEvent(options, state, event);
  }
}

/**
 * Forward one Discord REST page as a single durable application-ingestion
 * batch. Gateway events intentionally keep using the single-event path above;
 * batching is for archive reconciliation, where Discord already supplies a
 * natural page boundary and checkpoints make replay idempotent.
 */
export async function ingestObservedDiscordMessages(
  options: DiscordbotOptions,
  messages: readonly ObservedDiscordMessage[],
  state?: StateAdapter,
): Promise<void> {
  if (messages.length === 0) return;
  const events = deduplicateEvents(messages.flatMap(messageIngestionEvents));
  const first = messages[0]!;
  const last = messages.at(-1)!;
  await persistAndPostIngestionBatch(options, state, {
    source_key: `batch:${first.guildId}:${first.channelId}:${first.messageId}:${last.messageId}`,
    events,
  });
}

/** Discord signed CDN URLs carry an `ex` Unix timestamp encoded as hex. */
export function discordAttachmentExpiry(url: string): string | null {
  try {
    const encoded = new URL(url).searchParams.get("ex");
    if (!encoded || !/^[0-9a-f]+$/i.test(encoded)) return null;
    const seconds = Number.parseInt(encoded, 16);
    if (!Number.isSafeInteger(seconds) || seconds <= 0) return null;
    return new Date(seconds * 1000).toISOString();
  } catch {
    return null;
  }
}

export async function ingestDeletedDiscordMessage(
  options: DiscordbotOptions,
  message: { guildId: string; channelId: string; messageId: string; deletedAt: string },
  state?: StateAdapter,
): Promise<void> {
  await persistAndPostIngestionEvent(options, state, {
    guild_id: message.guildId,
    source_key: `message-delete:${message.messageId}:${message.deletedAt}`,
    type: "message_delete",
    channel_id: message.channelId,
    message_id: message.messageId,
    deleted_at: message.deletedAt,
  });
}

export async function ingestObservedDiscordChannel(
  options: DiscordbotOptions,
  channel: { guildId: string; channelId: string; name?: string; kind: string; parentId?: string; deleted: boolean },
  state?: StateAdapter,
): Promise<void> {
  await persistAndPostIngestionEvent(options, state, channel.deleted ? {
    guild_id: channel.guildId,
    source_key: `channel-delete:${channel.channelId}`,
    type: "channel_delete",
    channel_id: channel.channelId,
  } : {
    guild_id: channel.guildId,
    source_key: `channel:${channel.channelId}:${channel.name ?? ""}:${channel.kind}:${channel.parentId ?? ""}`,
    type: "channel_upsert",
    channel_id: channel.channelId,
    name: channel.name ?? null,
    kind: channel.kind,
    parent_id: channel.parentId ?? null,
  });
}

async function persistAndPostIngestionEvent(
  options: DiscordbotOptions,
  state: StateAdapter | undefined,
  payload: Record<string, unknown>,
): Promise<void> {
  if (!state) return postIngestionEvent(options, payload);
  const sourceKey = String(payload.source_key ?? "");
  if (!sourceKey) throw new Error("application ingestion event lacks source_key");
  const eventKey = `${INGESTION_EVENT_PREFIX}${sourceKey}`;
  await state.set(eventKey, payload, INGESTION_RETENTION_MS);
  await state.appendToList(INGESTION_INDEX_KEY, sourceKey, {
    maxLength: 100_000,
    ttlMs: INGESTION_RETENTION_MS,
  });
  // The durable outbox write above is the ingress contract. Application
  // delivery must not sit on the mention/typing critical path: Discord's
  // Gateway has already delivered the message and the application endpoint
  // can be arbitrarily slow. Deliver in the background and retain the outbox
  // entry on failure for the recovery loop. Source keys keep a recovery race
  // or process restart idempotent at the application boundary.
  void postIngestionEvent(options, payload)
    .then(() => state.delete(eventKey))
    .catch(() => undefined);
}

async function persistAndPostIngestionBatch(
  options: DiscordbotOptions,
  state: StateAdapter | undefined,
  payload: Record<string, unknown>,
): Promise<void> {
  if (!state) return postIngestionEvent(options, payload);
  const sourceKey = String(payload.source_key ?? "");
  if (!sourceKey) throw new Error("application ingestion batch lacks source_key");
  const eventKey = `${INGESTION_EVENT_PREFIX}${sourceKey}`;
  await state.set(eventKey, payload, INGESTION_RETENTION_MS);
  await state.appendToList(INGESTION_INDEX_KEY, sourceKey, {
    maxLength: 100_000,
    ttlMs: INGESTION_RETENTION_MS,
  });
  // Archive workers are already off the Gateway path. Await delivery here so
  // channel concurrency also bounds load on the private application. A failed
  // request remains in the durable outbox and prevents checkpoint advancement.
  await postIngestionEvent(options, payload);
  await state.delete(eventKey);
}

function messageIngestionEvents(
  message: ObservedDiscordMessage,
): Array<Record<string, unknown>> {
  const updatedAt = message.editedAt ?? message.createdAt;
  const events: Array<Record<string, unknown>> = [];
  if (message.authorName) {
    events.push({
      guild_id: message.guildId,
      source_key: `member:${message.authorId}:${message.authorName}:${message.displayName ?? ""}:${message.authorIsBot === true}`,
      type: "member_upsert",
      user_id: message.authorId,
      username: message.authorName,
      display_name: message.displayName ?? null,
      is_bot: message.authorIsBot === true,
    });
  }
  events.push({
    guild_id: message.guildId,
    source_key: `message:${message.messageId}:${updatedAt}`,
    type: "message_upsert",
    channel_id: message.threadId ?? message.channelId,
    message_id: message.messageId,
    thread_id: message.threadId ?? null,
    reply_to_message_id: message.replyToMessageId ?? null,
    author_user_id: message.authorId,
    content: message.content,
    normalized_payload: message.normalizedPayload ?? {},
    created_at: message.createdAt,
    updated_at: updatedAt,
    attachments: message.attachments.map((attachment) => ({
      id: attachment.id,
      filename: attachment.filename,
      content_type: attachment.contentType ?? null,
      size_bytes: attachment.size,
      url: attachment.url,
      url_expires_at: discordAttachmentExpiry(attachment.url),
    })),
  });
  return events;
}

function deduplicateEvents(
  events: Array<Record<string, unknown>>,
): Array<Record<string, unknown>> {
  return [...new Map(events.map((event) => [String(event.source_key), event])).values()];
}

export async function recoverApplicationIngestion(
  options: DiscordbotOptions,
  state: StateAdapter,
): Promise<number> {
  if (!options.applicationIngestionUrl || !options.applicationIngestionToken) return 0;
  const sourceKeys = Array.from(new Set(await state.getList<string>(INGESTION_INDEX_KEY)));
  let pending = 0;
  for (const sourceKey of sourceKeys) {
    const eventKey = `${INGESTION_EVENT_PREFIX}${sourceKey}`;
    const payload = await state.get<Record<string, unknown>>(eventKey);
    if (!payload) continue;
    try {
      await postIngestionEvent(options, payload);
      await state.delete(eventKey);
    } catch {
      pending += 1;
    }
  }
  return pending;
}

async function postIngestionEvent(
  options: DiscordbotOptions,
  payload: Record<string, unknown>,
): Promise<void> {
  if (!options.applicationIngestionUrl || !options.applicationIngestionToken) return;
  const fetchFn: DiscordbotFetch = options.fetch ?? fetch;
  const response = await fetchFn(options.applicationIngestionUrl, {
    method: "POST",
    headers: {
      authorization: `Bearer ${options.applicationIngestionToken}`,
      "content-type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    throw new Error(`application ingestion returned ${response.status}`);
  }
}
