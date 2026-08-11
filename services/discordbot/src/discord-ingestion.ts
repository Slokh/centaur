import type { DiscordbotFetch, DiscordbotOptions } from "./types";

export type ObservedDiscordMessage = {
  guildId: string;
  channelId: string;
  threadId?: string;
  messageId: string;
  authorId: string;
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
): Promise<void> {
  if (!options.applicationIngestionUrl || !options.applicationIngestionToken) {
    return;
  }
  const fetchFn: DiscordbotFetch = options.fetch ?? fetch;
  const updatedAt = message.editedAt ?? message.createdAt;
  const response = await fetchFn(options.applicationIngestionUrl, {
    method: "POST",
    headers: {
      authorization: `Bearer ${options.applicationIngestionToken}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      guild_id: message.guildId,
      source_key: `message:${message.messageId}:${updatedAt}`,
      type: "message_upsert",
      channel_id: message.threadId ?? message.channelId,
      message_id: message.messageId,
      thread_id: message.threadId ?? null,
      author_user_id: message.authorId,
      content: message.content,
      created_at: message.createdAt,
      updated_at: updatedAt,
      attachments: message.attachments.map((attachment) => ({
        id: attachment.id,
        filename: attachment.filename,
        content_type: attachment.contentType ?? null,
        size_bytes: attachment.size,
        url: attachment.url,
      })),
    }),
  });
  if (!response.ok) {
    throw new Error(`application ingestion returned ${response.status}`);
  }
}
