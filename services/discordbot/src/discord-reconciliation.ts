import type { StateAdapter } from "chat";
import {
  ingestObservedDiscordChannel,
  ingestObservedDiscordMessages,
  type ObservedDiscordMessage,
} from "./discord-ingestion";
import type { DiscordbotOptions } from "./types";

type DiscordChannel = {
  id: string;
  guild_id?: string;
  name?: string;
  type: number;
  parent_id?: string | null;
  thread_metadata?: { archive_timestamp?: string };
};
type DiscordMessage = {
  id: string;
  channel_id: string;
  content: string;
  timestamp: string;
  edited_timestamp?: string | null;
  type?: number;
  author: { id: string; username?: string; global_name?: string | null; bot?: boolean };
  member?: { nick?: string | null };
  message_reference?: { message_id?: string };
  embeds?: unknown[];
  poll?: unknown;
  sticker_items?: unknown[];
  attachments?: Array<{ id: string; filename: string; content_type?: string; size: number; url: string }>;
};

const CHECKPOINT_PREFIX = "discordbot:application-ingestion:channel-checkpoint:";
const BACKFILL_PREFIX = "discordbot:application-ingestion:channel-backfill:";
const BACKFILL_COMPLETE = "complete";
const ARCHIVED_THREAD_CURSOR_PREFIX = "discordbot:application-ingestion:archived-thread-cursor:";
const ARCHIVED_THREAD_SEEN_PREFIX = "discordbot:application-ingestion:archived-thread-seen:";
const PAGE_LIMIT = 10;
const DEFAULT_CHANNEL_CONCURRENCY = 4;
const MESSAGE_CHANNEL_TYPES = new Set([0, 5, 10, 11, 12]);
const THREAD_PARENT_CHANNEL_TYPES = new Set([0, 5, 15, 16]);

/**
 * Reconcile channels and messages through Discord REST after Gateway downtime.
 * Each channel advances an independent snowflake checkpoint only after its
 * normalized messages have been durably queued for Mindcool delivery.
 */
export async function reconcileDiscordArchive(
  options: DiscordbotOptions,
  state: StateAdapter,
): Promise<number> {
  if (!options.applicationIngestionUrl || !options.applicationIngestionToken) return 0;
  let observed = 0;
  for (const guildId of options.guildAllowlist ?? []) {
    const guildChannels = await discordGet<DiscordChannel[]>(options, `/guilds/${guildId}/channels`);
    const active = await discordGet<{ threads?: DiscordChannel[] }>(options, `/guilds/${guildId}/threads/active`);
    const channels = deduplicateChannels([...guildChannels, ...(active.threads ?? [])]);
    for (const channel of channels) {
      await ingestObservedDiscordChannel(options, {
        guildId,
        channelId: channel.id,
        name: channel.name,
        kind: String(channel.type),
        parentId: channel.parent_id ?? undefined,
        deleted: false,
      }, state);
    }
    const messageChannels = channels.filter((channel) =>
      MESSAGE_CHANNEL_TYPES.has(channel.type)
    );
    const counts = await mapConcurrent(
      messageChannels,
      Math.min(
        16,
        Math.max(
          1,
          options.applicationArchiveReconciliationConcurrency ??
            DEFAULT_CHANNEL_CONCURRENCY,
        ),
      ),
      async (channel) => {
        try {
          return await reconcileChannel(options, state, guildId, channel);
        } catch (error) {
          if (
            error instanceof DiscordReconciliationError &&
            (error.status === 403 || error.status === 404)
          ) {
            options.logger?.warn("discordbot_archive_channel_skipped", {
              channel_id: channel.id,
              status: error.status,
            });
            return 0;
          }
          throw error;
        }
      },
    );
    observed += counts.reduce((total, count) => total + count, 0);
    observed += await reconcileArchivedPublicThreads(
      options,
      state,
      guildId,
      guildChannels,
    );
  }
  return observed;
}

async function reconcileArchivedPublicThreads(
  options: DiscordbotOptions,
  state: StateAdapter,
  guildId: string,
  guildChannels: DiscordChannel[],
): Promise<number> {
  let observed = 0;
  for (const parent of guildChannels.filter((channel) => THREAD_PARENT_CHANNEL_TYPES.has(channel.type))) {
    const cursorKey = `${ARCHIVED_THREAD_CURSOR_PREFIX}${guildId}:${parent.id}`;
    let cursor = await state.get<string>(cursorKey);
    const latest = await getArchivedPublicThreads(options, parent.id);
    observed += await reconcileUnseenArchivedThreads(
      options,
      state,
      guildId,
      latest.threads,
    );
    if (!cursor) {
      cursor = latest.has_more ? archiveTimestamp(latest.threads.at(-1)) : BACKFILL_COMPLETE;
      // Advance only after every thread on the page has been durably queued and
      // reconciled. A crash before this write safely replays the whole page.
      await state.set(cursorKey, cursor);
    }
    for (
      let page = 0;
      cursor !== BACKFILL_COMPLETE && page < PAGE_LIMIT;
      page += 1
    ) {
      const result = await getArchivedPublicThreads(options, parent.id, cursor);
      observed += await reconcileUnseenArchivedThreads(
        options,
        state,
        guildId,
        result.threads,
      );
      cursor = result.has_more ? archiveTimestamp(result.threads.at(-1)) : BACKFILL_COMPLETE;
      await state.set(cursorKey, cursor);
    }
  }
  return observed;
}

async function reconcileUnseenArchivedThreads(
  options: DiscordbotOptions,
  state: StateAdapter,
  guildId: string,
  threads: DiscordChannel[],
): Promise<number> {
  let observed = 0;
  for (const thread of threads) {
    const seenKey = `${ARCHIVED_THREAD_SEEN_PREFIX}${guildId}:${thread.id}`;
    if (await state.get<boolean>(seenKey)) continue;
    await ingestObservedDiscordChannel(
      options,
      {
        guildId,
        channelId: thread.id,
        name: thread.name,
        kind: String(thread.type),
        parentId: thread.parent_id ?? undefined,
        deleted: false,
      },
      state,
    );
    observed += await reconcileChannel(options, state, guildId, thread);
    // Marking seen is the final step. Failure before this point replays an
    // idempotent channel/message reconciliation on the next pass.
    await state.set(seenKey, true);
  }
  return observed;
}

function getArchivedPublicThreads(
  options: DiscordbotOptions,
  channelId: string,
  before?: string,
): Promise<{ threads: DiscordChannel[]; has_more: boolean }> {
  const params = new URLSearchParams({ limit: "100" });
  if (before) params.set("before", before);
  return discordGet(options, `/channels/${channelId}/threads/archived/public?${params}`);
}

function archiveTimestamp(thread: DiscordChannel | undefined): string {
  const timestamp = thread?.thread_metadata?.archive_timestamp;
  if (!timestamp) throw new Error("Discord archived-thread page lacked an archive timestamp");
  return timestamp;
}

function deduplicateChannels(channels: DiscordChannel[]): DiscordChannel[] {
  return [...new Map(channels.map((channel) => [channel.id, channel])).values()];
}

async function reconcileChannel(
  options: DiscordbotOptions,
  state: StateAdapter,
  guildId: string,
  channel: DiscordChannel,
): Promise<number> {
  const checkpointKey = `${CHECKPOINT_PREFIX}${guildId}:${channel.id}`;
  const backfillKey = `${BACKFILL_PREFIX}${guildId}:${channel.id}`;
  let after = await state.get<string>(checkpointKey);
  let observed = 0;
  if (!after) {
    const messages = await getMessages(options, channel.id);
    if (messages.length === 0) {
      await state.set(backfillKey, BACKFILL_COMPLETE);
      return 0;
    }
    messages.sort((left, right) => compareSnowflakes(left.id, right.id));
    await ingestObservedDiscordMessages(
      options,
      messages.map((message) => normalizeMessage(guildId, channel, message)),
      state,
    );
    observed += messages.length;
    after = messages.at(-1)!.id;
    await state.set(checkpointKey, after);
    await state.set(
      backfillKey,
      messages.length < 100 ? BACKFILL_COMPLETE : messages[0]!.id,
    );
  }

  for (let page = 0; page < PAGE_LIMIT; page += 1) {
    const params = new URLSearchParams({ limit: "100" });
    params.set("after", after);
    const messages = await getMessages(options, channel.id, params);
    if (messages.length === 0) break;
    messages.sort((left, right) => compareSnowflakes(left.id, right.id));
    await ingestObservedDiscordMessages(
      options,
      messages.map((message) => normalizeMessage(guildId, channel, message)),
      state,
    );
    after = messages.at(-1)!.id;
    await state.set(checkpointKey, after);
    observed += messages.length;
    if (messages.length < 100) break;
  }

  let before = await state.get<string>(backfillKey);
  for (
    let page = 0;
    before && before !== BACKFILL_COMPLETE && page < PAGE_LIMIT;
    page += 1
  ) {
    const params = new URLSearchParams({ limit: "100", before });
    const messages = await getMessages(options, channel.id, params);
    if (messages.length === 0) {
      before = BACKFILL_COMPLETE;
      await state.set(backfillKey, before);
      break;
    }
    messages.sort((left, right) => compareSnowflakes(left.id, right.id));
    await ingestObservedDiscordMessages(
      options,
      messages.map((message) => normalizeMessage(guildId, channel, message)),
      state,
    );
    observed += messages.length;
    before = messages.length < 100 ? BACKFILL_COMPLETE : messages[0]!.id;
    await state.set(backfillKey, before);
  }
  return observed;
}

function getMessages(
  options: DiscordbotOptions,
  channelId: string,
  params = new URLSearchParams({ limit: "100" }),
): Promise<DiscordMessage[]> {
  return discordGet(options, `/channels/${channelId}/messages?${params}`);
}

function normalizeMessage(
  guildId: string,
  channel: DiscordChannel,
  message: DiscordMessage,
): ObservedDiscordMessage {
  const isThread = [10, 11, 12].includes(channel.type);
  return {
    guildId,
    channelId: isThread ? channel.parent_id ?? channel.id : channel.id,
    threadId: isThread ? channel.id : undefined,
    messageId: message.id,
    authorId: message.author.id,
    authorName: message.author.username,
    displayName: message.member?.nick ?? message.author.global_name ?? undefined,
    authorIsBot: message.author.bot === true,
    replyToMessageId: message.message_reference?.message_id,
    content: message.content,
    createdAt: message.timestamp,
    editedAt: message.edited_timestamp ?? undefined,
    normalizedPayload: {
      embeds: message.embeds ?? [],
      poll: message.poll ?? null,
      stickers: message.sticker_items ?? [],
      messageType: message.type,
    },
    attachments: (message.attachments ?? []).map((attachment) => ({
      id: attachment.id,
      filename: attachment.filename,
      contentType: attachment.content_type,
      size: attachment.size,
      url: attachment.url,
    })),
  };
}

async function discordGet<T>(options: DiscordbotOptions, path: string): Promise<T> {
  const fetchFn = options.fetch ?? fetch;
  for (let attempt = 0; attempt < 5; attempt += 1) {
    const response = await fetchFn(`${options.discordApiUrl ?? "https://discord.com/api/v10"}${path}`, {
      headers: { authorization: `Bot ${options.botToken}` },
    });
    if (response.ok) return response.json() as Promise<T>;
    if (response.status !== 429 || attempt === 4) {
      throw new DiscordReconciliationError(response.status, path);
    }
    const payload = await response.clone().json().catch(() => ({})) as {
      retry_after?: unknown;
    };
    const retrySeconds = typeof payload.retry_after === "number"
      ? payload.retry_after
      : 1;
    await Bun.sleep(Math.max(25, Math.ceil(retrySeconds * 1_000)));
  }
  throw new Error(`Discord reconciliation exhausted retries for ${path}`);
}

class DiscordReconciliationError extends Error {
  constructor(
    readonly status: number,
    readonly path: string,
  ) {
    super(`Discord reconciliation returned ${status} for ${path}`);
    this.name = "DiscordReconciliationError";
  }
}

async function mapConcurrent<T, R>(
  values: readonly T[],
  concurrency: number,
  work: (value: T) => Promise<R>,
): Promise<R[]> {
  const results = new Array<R>(values.length);
  let next = 0;
  const workers = Array.from(
    { length: Math.min(concurrency, values.length) },
    async () => {
      while (next < values.length) {
        const index = next;
        next += 1;
        results[index] = await work(values[index]!);
      }
    },
  );
  await Promise.all(workers);
  return results;
}

function compareSnowflakes(left: string, right: string): number {
  const a = BigInt(left);
  const b = BigInt(right);
  return a < b ? -1 : a > b ? 1 : 0;
}
