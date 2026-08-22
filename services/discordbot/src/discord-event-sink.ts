import type { DiscordbotFetch, DiscordbotOptions } from "./types";
import type {
  DiscordEventSinkClaim,
  DiscordEventSinkOutbox,
} from "./discord-event-sink-outbox";
import { discardResponseBody } from "./utils";

const DEFAULT_DELIVERY_TIMEOUT_MS = 15_000;
const DEFAULT_RECOVERY_BATCH_SIZE = 25;
const DEFAULT_RECOVERY_CONCURRENCY = 4;
const DEFAULT_RECOVERY_LEASE_MS = 5 * 60_000;
const MAX_RETRY_DELAY_MS = 15 * 60_000;

export type DiscordEventSinkEnvelope = {
  version: 1;
  source: "discord";
  source_key: string;
  event: Record<string, unknown>;
};

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
  outbox: DiscordEventSinkOutbox | undefined =
    options.eventSinkOutbox,
): Promise<void> {
  if (!options.eventSinkUrl || !options.eventSinkToken) {
    return;
  }
  for (const event of messageIngestionEvents(message)) {
    await persistIngestionEvent(options, outbox, event);
  }
}

/**
 * Forward one Discord REST page as a single durable event-sink
 * batch. Gateway events intentionally keep using the single-event path above;
 * batching is for archive reconciliation, where Discord already supplies a
 * natural page boundary and checkpoints make replay idempotent.
 */
export async function ingestObservedDiscordMessages(
  options: DiscordbotOptions,
  messages: readonly ObservedDiscordMessage[],
  outbox: DiscordEventSinkOutbox | undefined =
    options.eventSinkOutbox,
): Promise<void> {
  if (messages.length === 0) return;
  const events = deduplicateEvents(messages.flatMap(messageIngestionEvents));
  const first = messages[0]!;
  const last = messages.at(-1)!;
  await persistIngestionEvent(options, outbox, {
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
  outbox: DiscordEventSinkOutbox | undefined =
    options.eventSinkOutbox,
): Promise<void> {
  await persistIngestionEvent(options, outbox, {
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
  outbox: DiscordEventSinkOutbox | undefined =
    options.eventSinkOutbox,
): Promise<void> {
  await persistIngestionEvent(options, outbox, channel.deleted ? {
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

async function persistIngestionEvent(
  options: DiscordbotOptions,
  outbox: DiscordEventSinkOutbox | undefined,
  payload: Record<string, unknown>,
): Promise<void> {
  if (!outbox) return postIngestionEvent(options, payload);
  const sourceKey = String(payload.source_key ?? "");
  if (!sourceKey) throw new Error("Discord event sink event lacks source_key");
  await outbox.enqueue(sourceKey, payload);
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

export type DiscordEventSinkRecoveryResult = {
  claimed: number;
  delivered: number;
  failed: number;
};

export async function recoverDiscordEventSink(
  options: DiscordbotOptions,
  outbox: DiscordEventSinkOutbox = requiredOutbox(options),
): Promise<DiscordEventSinkRecoveryResult> {
  if (!options.eventSinkUrl || !options.eventSinkToken) {
    return { claimed: 0, delivered: 0, failed: 0 };
  }
  const claims = await outbox.claim(
    options.eventSinkRecoveryBatchSize ??
      DEFAULT_RECOVERY_BATCH_SIZE,
    options.eventSinkRecoveryLeaseMs ?? DEFAULT_RECOVERY_LEASE_MS,
  );
  let delivered = 0;
  let failed = 0;
  const concurrency = Math.max(
    1,
    Math.min(
      claims.length,
      options.eventSinkRecoveryConcurrency ??
        DEFAULT_RECOVERY_CONCURRENCY,
    ),
  );
  let cursor = 0;
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      for (;;) {
        const claim = claims[cursor++];
        if (!claim) return;
        if (await deliverClaim(options, outbox, claim)) delivered += 1;
        else failed += 1;
      }
    }),
  );
  return {
    claimed: claims.length,
    delivered,
    failed,
  };
}

async function postIngestionEvent(
  options: DiscordbotOptions,
  payload: Record<string, unknown>,
): Promise<void> {
  if (!options.eventSinkUrl || !options.eventSinkToken) return;
  const fetchFn: DiscordbotFetch = options.fetch ?? fetch;
  const timeoutMs =
    options.eventSinkDeliveryTimeoutMs ??
    DEFAULT_DELIVERY_TIMEOUT_MS;
  const response = await fetchFn(options.eventSinkUrl, {
    method: "POST",
    headers: {
      authorization: `Bearer ${options.eventSinkToken}`,
      "content-type": "application/json",
    },
    body: JSON.stringify(eventSinkEnvelope(payload)),
    signal: AbortSignal.timeout(timeoutMs),
  });
  await discardResponseBody(response);
  if (!response.ok) {
    throw new Error(`Discord event sink returned ${response.status}`);
  }
}

function eventSinkEnvelope(
  payload: Record<string, unknown>,
): DiscordEventSinkEnvelope {
  const sourceKey = String(payload.source_key ?? "");
  if (!sourceKey) throw new Error("Discord event sink payload lacks source_key");
  return {
    version: 1,
    source: "discord",
    source_key: sourceKey,
    event: payload,
  };
}

async function deliverClaim(
  options: DiscordbotOptions,
  outbox: DiscordEventSinkOutbox,
  claim: DiscordEventSinkClaim,
): Promise<boolean> {
  try {
    await postIngestionEvent(options, claim.payload);
    return await outbox.acknowledge(claim);
  } catch (error) {
    await outbox.retry(
      claim,
      retryDelayMs(claim.attempt),
      error instanceof Error ? error.message : String(error),
    );
    return false;
  }
}

function retryDelayMs(attempt: number): number {
  return Math.min(1_000 * 2 ** Math.min(Math.max(attempt - 1, 0), 10), MAX_RETRY_DELAY_MS);
}

function requiredOutbox(options: DiscordbotOptions): DiscordEventSinkOutbox {
  if (!options.eventSinkOutbox) {
    throw new Error("Discord event sink recovery requires a durable outbox");
  }
  return options.eventSinkOutbox;
}
