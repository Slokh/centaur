import { describe, expect, it } from "bun:test";
import { ingestObservedDiscordMessage } from "../src/discord-ingestion";
import type { DiscordbotOptions } from "../src/types";

describe("Discord application ingestion", () => {
  it("normalizes a thread message and sends an idempotent source key", async () => {
    let request: Request | undefined;
    const options = {
      apiUrl: "http://centaur",
      applicationId: "app",
      botToken: "bot",
      publicKey: "key",
      applicationIngestionUrl: "http://mindcool/v1/discord/events",
      applicationIngestionToken: "ingest-secret",
      fetch: async (input: RequestInfo | URL, init?: RequestInit) => {
        request = new Request(input, init);
        return new Response("{}", { status: 200 });
      },
    } satisfies DiscordbotOptions;

    await ingestObservedDiscordMessage(options, {
      guildId: "G1",
      channelId: "C1",
      threadId: "T1",
      messageId: "M1",
      authorId: "U1",
      content: "hello",
      createdAt: "2026-08-10T10:00:00.000Z",
      attachments: [],
    });

    expect(request?.headers.get("authorization")).toBe(
      "Bearer ingest-secret",
    );
    expect(await request?.json()).toMatchObject({
      source_key: "message:M1:2026-08-10T10:00:00.000Z",
      channel_id: "T1",
      thread_id: "T1",
      message_id: "M1",
    });
  });
});
