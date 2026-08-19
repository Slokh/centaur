import type { Attachment, Logger } from "chat";
import { parseDiscordThreadKey } from "./discord-allowlist";
import { DEFAULT_DISCORD_API_URL } from "./discord-threading";
import type {
  DiscordbotApiAttachment,
  DiscordbotApiMessage,
  DiscordbotOptions,
  JsonObject,
} from "./types";
import { isJsonObject } from "./utils";

/**
 * Discord delta with no slackbotv2 analog: a thread created from a message keeps
 * that starter message in the **parent channel** (the thread shares its ID), so it
 * never appears in the thread's own history — Slack's conversations.replies
 * returns the parent as the first reply, but Discord requires this extra fetch
 * (mirrors discord.js `ThreadChannel#fetchStarterMessage`).
 *
 * Returns null when the key has no thread segment, the thread was not created
 * from a message (404), or on any failure — context enrichment must never block
 * execution.
 */
export async function fetchThreadStarterMessage(
  options: DiscordbotOptions,
  threadKey: string,
  logger: Logger,
): Promise<DiscordbotApiMessage | null> {
  const { channelId, threadId } = parseDiscordThreadKey(threadKey);
  if (!channelId || !threadId) return null;

  const fetchFn = options.fetch ?? fetch;
  const apiBase = (options.discordApiUrl ?? DEFAULT_DISCORD_API_URL).replace(
    /\/$/,
    "",
  );
  try {
    const response = await fetchFn(
      `${apiBase}/channels/${channelId}/messages/${threadId}`,
      { headers: { authorization: `Bot ${options.botToken}` } },
    );
    if (!response.ok) {
      // 404 = the thread was created standalone ("+ New Thread") or the
      // starter message was deleted; both are normal, not errors.
      if (response.status !== 404) {
        logger.warn("discordbot_thread_starter_fetch_failed", {
          status: response.status,
          thread_id: threadId,
        });
      }
      return null;
    }
    return rawMessageToApiMessage(
      await response.json(),
      threadKey,
      options.applicationId,
    );
  } catch (error) {
    logger.warn("discordbot_thread_starter_fetch_error", {
      error: error instanceof Error ? error.message : String(error),
      thread_id: threadId,
    });
    return null;
  }
}

/** Recover the bounded visible reply chain when an inline session starts mid-chain. */
export async function fetchInlineReplyContext(
  options: DiscordbotOptions,
  threadKey: string,
  currentMessage: DiscordbotApiMessage,
  logger: Logger,
): Promise<DiscordbotApiMessage[]> {
  const parsed = parseDiscordThreadKey(threadKey);
  if (!parsed.replyToMessageId || !isJsonObject(currentMessage.raw)) {
    return [currentMessage];
  }

  const fetchFn = options.fetch ?? fetch;
  const apiBase = (options.discordApiUrl ?? DEFAULT_DISCORD_API_URL).replace(
    /\/$/,
    "",
  );
  const messages = [currentMessage];
  const visited = new Set<string>([currentMessage.id]);
  let cursor = currentMessage.raw;

  for (let depth = 0; depth < 32; depth += 1) {
    const reference = isJsonObject(cursor.message_reference)
      ? cursor.message_reference
      : undefined;
    const parentId = nonEmptyString(reference?.message_id);
    if (!parentId || visited.has(parentId)) break;

    // A forward's immutable snapshot is visible in the destination and is
    // flattened below. Do not cross into its source channel using bot authority.
    if (hasForwardedSnapshot(cursor)) break;

    const embeddedParent = isJsonObject(cursor.referenced_message)
      ? cursor.referenced_message
      : undefined;
    let parent: JsonObject | undefined;
    if (embeddedParent && embeddedParent.id === parentId) {
      parent = embeddedParent;
    } else {
      const channelId =
        nonEmptyString(reference?.channel_id) ??
        nonEmptyString(cursor.channel_id) ??
        parsed.channelId;
      if (!channelId) break;
      try {
        const response = await fetchFn(
          `${apiBase}/channels/${channelId}/messages/${parentId}`,
          { headers: { authorization: `Bot ${options.botToken}` } },
        );
        if (!response.ok) {
          logger.warn("discordbot_inline_reply_parent_fetch_failed", {
            message_id: parentId,
            status: response.status,
          });
          break;
        }
        const raw: unknown = await response.json();
        if (!isJsonObject(raw)) break;
        parent = raw;
      } catch (error) {
        logger.warn("discordbot_inline_reply_parent_fetch_error", {
          error: error instanceof Error ? error.message : String(error),
          message_id: parentId,
        });
        break;
      }
    }

    const serialized = rawMessageToApiMessage(
      parent,
      threadKey,
      options.applicationId,
    );
    if (!serialized) break;
    messages.push(serialized);
    visited.add(parentId);
    cursor = parent;
  }

  return messages.reverse();
}

/**
 * Append flattened embed content to a message's text. Webhook-style messages
 * (Sentry alerts, GitHub notifications) carry their payload entirely in
 * `embeds` with empty `content`; the chat adapter only surfaces `content`, so
 * without this the agent sees an empty message.
 */
export function withDiscordMessageText(text: string, raw: unknown): string {
  if (!isJsonObject(raw)) return text;
  const snapshots = Array.isArray(raw.message_snapshots)
    ? raw.message_snapshots
    : [];
  const snapshotText = snapshots
    .map((snapshot) => {
      if (!isJsonObject(snapshot) || !isJsonObject(snapshot.message)) return "";
      const message = snapshot.message;
      return withDiscordMessageText(
        nonEmptyString(message.content) ?? "",
        message,
      );
    })
    .filter(Boolean)
    .map((snapshot) => `[forwarded message] ${snapshot}`)
    .join("\n\n");
  const embeds = Array.isArray(raw.embeds) ? raw.embeds : [];
  const embedText = embeds
    .map((embed) => embedToText(embed))
    .filter(Boolean)
    .join("\n\n");
  return [text.trim(), snapshotText, embedText].filter(Boolean).join("\n\n");
}

/** @deprecated Use withDiscordMessageText for all structured Discord text. */
export const withDiscordEmbedText = withDiscordMessageText;

function hasForwardedSnapshot(raw: JsonObject): boolean {
  return Array.isArray(raw.message_snapshots) && raw.message_snapshots.length > 0;
}

function embedToText(embed: unknown): string {
  if (!isJsonObject(embed)) return "";
  const lines: string[] = [];
  const author = isJsonObject(embed.author)
    ? nonEmptyString(embed.author.name)
    : undefined;
  if (author) lines.push(author);
  const title = nonEmptyString(embed.title);
  const url = nonEmptyString(embed.url);
  if (title) {
    lines.push(url ? `${title} (${url})` : title);
  } else if (url) {
    lines.push(url);
  }
  const description = nonEmptyString(embed.description);
  if (description) lines.push(description);
  const fields = Array.isArray(embed.fields) ? embed.fields : [];
  for (const field of fields) {
    if (!isJsonObject(field)) continue;
    const name = nonEmptyString(field.name);
    const value = nonEmptyString(field.value);
    if (name && value) lines.push(`${name}: ${value}`);
    else if (value) lines.push(value);
  }
  const footer = isJsonObject(embed.footer)
    ? nonEmptyString(embed.footer.text)
    : undefined;
  if (footer) lines.push(footer);
  if (lines.length === 0) return "";
  return `[embed] ${lines.join("\n")}`;
}

export function rawMessageToApiMessage(
  raw: unknown,
  threadKey: string,
  botUserId: string,
): DiscordbotApiMessage | null {
  if (!isJsonObject(raw) || typeof raw.id !== "string") return null;
  const author = isJsonObject(raw.author) ? raw.author : {};
  const userId = nonEmptyString(author.id) ?? "unknown";
  const userName = nonEmptyString(author.username) ?? "unknown";
  return {
    attachments: rawAttachments(raw),
    author: {
      fullName: nonEmptyString(author.global_name) ?? userName,
      isBot: author.bot === true,
      isMe: userId === botUserId,
      userId,
      userName,
    },
    id: raw.id,
    isMention: false,
    raw,
    text: withDiscordMessageText(nonEmptyString(raw.content) ?? "", raw),
    threadId: threadKey,
    timestamp: nonEmptyString(raw.timestamp) ?? "",
  };
}

function rawAttachments(raw: JsonObject): DiscordbotApiAttachment[] {
  const attachments = Array.isArray(raw.attachments) ? raw.attachments : [];
  return attachments.filter(isJsonObject).map((attachment) => ({
    height: numberValue(attachment.height),
    mimeType: nonEmptyString(attachment.content_type),
    name: nonEmptyString(attachment.filename),
    size: numberValue(attachment.size),
    type: attachmentType(nonEmptyString(attachment.content_type)),
    url: nonEmptyString(attachment.url),
    width: numberValue(attachment.width),
  }));
}

// Mirrors the chat adapter's getAttachmentType MIME mapping.
function attachmentType(mimeType: string | undefined): Attachment["type"] {
  if (mimeType?.startsWith("image/")) return "image";
  if (mimeType?.startsWith("video/")) return "video";
  if (mimeType?.startsWith("audio/")) return "audio";
  return "file";
}

function nonEmptyString(value: unknown): string | undefined {
  return typeof value === "string" && value ? value : undefined;
}

function numberValue(value: unknown): number | undefined {
  return typeof value === "number" ? value : undefined;
}
