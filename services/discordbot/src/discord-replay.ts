import { DEFAULT_DISCORD_API_URL } from "./discord-threading";
import { resolveDiscordVisibleChannelIds } from "./discord-visibility";

type DiscordAuthor = {
  bot?: boolean;
  global_name?: string | null;
  id: string;
  username?: string;
};

type DiscordMessage = {
  attachments?: Array<{ content_type?: string; filename?: string; id: string }>;
  author: DiscordAuthor;
  channel_id: string;
  content?: string;
  id: string;
  message_reference?: { message_id?: string };
  timestamp: string;
};

type DiscordChannel = { id: string; parent_id?: string | null; type: number };

export type DiscordReplayContext = {
  channel_id: string;
  guild_id: string;
  messages: Array<{
    author_name: string;
    id: string;
    role: "assistant" | "user";
    text: string;
    timestamp: string;
    user_id: string;
  }>;
  source_message_id: string;
  thread_id: string | null;
  user_id: string;
  visible_channel_ids: string[];
};

export async function resolveDiscordReplayContext(input: {
  apiUrl?: string;
  applicationId: string;
  botToken: string;
  fetch?: typeof fetch;
  reference: string;
  userId?: string;
}): Promise<DiscordReplayContext> {
  const parsed = parseDiscordPermalink(input.reference);
  const apiBase = (input.apiUrl ?? DEFAULT_DISCORD_API_URL).replace(/\/$/, "");
  const fetchFn = input.fetch ?? fetch;
  const headers = { authorization: `Bot ${input.botToken}` };
  const getJson = async <T>(path: string): Promise<T> => {
    for (let attempt = 0; attempt < 4; attempt += 1) {
      const response = await fetchFn(`${apiBase}${path}`, { headers });
      if (response.ok) return (await response.json()) as T;
      if (response.status === 429 && attempt < 3) {
        const body = (await response.json().catch(() => ({}))) as {
          retry_after?: number;
        };
        const retryAfterMs = Math.max(
          50,
          Math.min(30_000, Math.ceil((body.retry_after ?? 1) * 1_000)),
        );
        await new Promise((resolve) => setTimeout(resolve, retryAfterMs));
        continue;
      }
      throw new Error(`Discord replay lookup failed (${response.status})`);
    }
    throw new Error("Discord replay lookup exhausted its retries");
  };

  const target = await getJson<DiscordMessage>(
    `/channels/${parsed.channelId}/messages/${parsed.messageId}`,
  );
  const channel = await getJson<DiscordChannel>(
    `/channels/${parsed.channelId}`,
  );
  const ancestors: DiscordMessage[] = [];
  let cursor = target;
  const visited = new Set<string>([target.id]);
  for (let depth = 0; depth < 50; depth += 1) {
    const parentId = cursor.message_reference?.message_id;
    if (!parentId || visited.has(parentId)) break;
    const parent = await getJson<DiscordMessage>(
      `/channels/${parsed.channelId}/messages/${parentId}`,
    );
    ancestors.push(parent);
    visited.add(parent.id);
    cursor = parent;
  }
  const root = ancestors.at(-1) ?? target;
  const nearby = await getJson<DiscordMessage[]>(
    `/channels/${parsed.channelId}/messages?after=${root.id}&limit=100`,
  );
  const ordered = [root, ...nearby]
    .filter((message, index, all) =>
      all.findIndex((candidate) => candidate.id === message.id) === index &&
      BigInt(message.id) <= BigInt(target.id),
    )
    .sort((left, right) => (BigInt(left.id) < BigInt(right.id) ? -1 : 1));
  const chainIds = new Set<string>([root.id]);
  const chain: DiscordMessage[] = [root];
  for (const message of ordered.slice(1)) {
    const parentId = message.message_reference?.message_id;
    if (message.id === target.id || (parentId && chainIds.has(parentId))) {
      chain.push(message);
      chainIds.add(message.id);
    }
  }
  if (!chainIds.has(target.id)) chain.push(target);

  const targetIsAgent = target.author.id === input.applicationId;
  const source = targetIsAgent
    ? chain
        .filter(
          (message) =>
            BigInt(message.id) < BigInt(target.id) &&
            message.author.id !== input.applicationId,
        )
        .at(-1)
    : target;
  if (!source) throw new Error("could not identify the triggering Discord message");
  const replayMessages = chain.filter(
    (message) => BigInt(message.id) <= BigInt(source.id),
  );
  const parentChannelId = channel.parent_id ?? parsed.channelId;
  const threadId = channel.parent_id ? parsed.channelId : undefined;
  const userId = input.userId ?? source.author.id;
  const visibleChannelIds = await resolveDiscordVisibleChannelIds({
    apiUrl: input.apiUrl,
    botToken: input.botToken,
    currentChannelId: parentChannelId,
    ...(threadId ? { currentThreadId: threadId } : {}),
    fetch: fetchFn,
    guildId: parsed.guildId,
    userId,
  });
  return {
    channel_id: parentChannelId,
    guild_id: parsed.guildId,
    messages: replayMessages.map((message) => ({
      author_name:
        message.author.global_name ??
        message.author.username ??
        message.author.id,
      id: message.id,
      role: message.author.id === input.applicationId ? "assistant" : "user",
      text: replayText(message),
      timestamp: message.timestamp,
      user_id: message.author.id,
    })),
    source_message_id: source.id,
    thread_id: threadId ?? null,
    user_id: userId,
    visible_channel_ids: visibleChannelIds,
  };
}

export function parseDiscordPermalink(reference: string): {
  channelId: string;
  guildId: string;
  messageId: string;
} {
  const match = reference.match(
    /^https:\/\/(?:canary\.|ptb\.)?discord(?:app)?\.com\/channels\/(\d+)\/(\d+)\/(\d+)(?:[/?#].*)?$/,
  );
  if (!match) throw new Error("expected a Discord message permalink");
  return { guildId: match[1]!, channelId: match[2]!, messageId: match[3]! };
}

function replayText(message: DiscordMessage): string {
  const content = message.content?.trim() ?? "";
  const attachments = (message.attachments ?? [])
    .map((attachment) =>
      `[attachment: ${attachment.filename ?? attachment.id}${
        attachment.content_type ? ` (${attachment.content_type})` : ""
      }]`,
    )
    .join("\n");
  return [content, attachments].filter(Boolean).join("\n");
}
