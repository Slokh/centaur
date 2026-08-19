import { describe, expect, it } from "bun:test";
import { createMemoryState } from "@chat-adapter/state-memory";
import {
  discordAttachmentExpiry,
  ingestObservedDiscordMessage,
  ingestObservedDiscordMessages,
  recoverApplicationIngestion,
} from "../src/discord-ingestion";
import type { DiscordbotOptions } from "../src/types";

describe("Discord application ingestion", () => {
  it("discards successful application response bodies", async () => {
    let cancelled = false;
    const options = {
      apiUrl: "http://centaur",
      applicationId: "app",
      botToken: "bot",
      publicKey: "key",
      applicationIngestionUrl: "http://application.local/v1/discord/events",
      applicationIngestionToken: "ingest-secret",
      fetch: async () => new Response(new ReadableStream({
        cancel() {
          cancelled = true;
        },
      }), { status: 200 }),
    } satisfies DiscordbotOptions;

    await ingestObservedDiscordMessage(options, {
      guildId: "G1",
      channelId: "C1",
      messageId: "M0",
      authorId: "U1",
      content: "release the response",
      createdAt: "2026-08-10T10:00:00.000Z",
      attachments: [],
    });

    expect(cancelled).toBeTrue();
  });

  it("normalizes a thread message and sends an idempotent source key", async () => {
    let request: Request | undefined;
    const options = {
      apiUrl: "http://centaur",
      applicationId: "app",
      botToken: "bot",
      publicKey: "key",
      applicationIngestionUrl: "http://application.local/v1/discord/events",
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

  it("durably retries an event after the application is unavailable", async () => {
    const state = createMemoryState();
    await state.connect();
    let available = false;
    let delivered = 0;
    const options = {
      apiUrl: "http://centaur",
      applicationId: "app",
      botToken: "bot",
      publicKey: "key",
      applicationIngestionUrl: "http://application.local/v1/discord/events",
      applicationIngestionToken: "ingest-secret",
      fetch: async () => {
        delivered += 1;
        return new Response("{}", { status: available ? 200 : 503 });
      },
    } satisfies DiscordbotOptions;

    await ingestObservedDiscordMessage(
      options,
      {
        guildId: "G1",
        channelId: "C1",
        messageId: "M2",
        authorId: "U1",
        content: "persist me",
        createdAt: "2026-08-10T10:00:00.000Z",
        attachments: [],
      },
      state,
    );
    expect(delivered).toBe(1);

    // Application delivery is deliberately detached from the Gateway path;
    // let the failed background attempt leave its durable outbox entry.
    await Bun.sleep(0);

    available = true;
    expect(await recoverApplicationIngestion(options, state)).toBe(0);
    expect(delivered).toBe(2);
    expect(await recoverApplicationIngestion(options, state)).toBe(0);
    expect(delivered).toBe(2);
  });

  it("does not wait for slow application delivery after writing the outbox", async () => {
    const state = createMemoryState();
    await state.connect();
    let releaseDelivery: (() => void) | undefined;
    const deliveryBlocked = new Promise<void>((resolve) => {
      releaseDelivery = resolve;
    });
    const options = {
      apiUrl: "http://centaur",
      applicationId: "app",
      botToken: "bot",
      publicKey: "key",
      applicationIngestionUrl: "http://application.local/v1/discord/events",
      applicationIngestionToken: "ingest-secret",
      fetch: async () => {
        await deliveryBlocked;
        return new Response("{}", { status: 200 });
      },
    } satisfies DiscordbotOptions;

    const ingestion = ingestObservedDiscordMessage(
      options,
      {
        guildId: "G1",
        channelId: "C1",
        messageId: "M3",
        authorId: "U1",
        content: "ack me first",
        createdAt: "2026-08-10T10:00:00.000Z",
        attachments: [],
      },
      state,
    );

    await expect(
      Promise.race([
        ingestion.then(() => "returned"),
        Bun.sleep(100).then(() => "blocked"),
      ]),
    ).resolves.toBe("returned");
    releaseDelivery?.();
    await Bun.sleep(0);
  });

  it("decodes Discord signed attachment expiry", () => {
    expect(discordAttachmentExpiry("https://cdn.discordapp.com/a?ex=65c00000"))
      .toBe(new Date(Number.parseInt("65c00000", 16) * 1000).toISOString());
    expect(discordAttachmentExpiry("not a url")).toBeNull();
  });

  it("sends one archive batch and deduplicates repeated member events", async () => {
    let payload: { events?: Record<string, unknown>[] } | undefined;
    const options = {
      apiUrl: "http://centaur",
      applicationId: "app",
      botToken: "bot",
      publicKey: "key",
      applicationIngestionUrl: "http://application.local/v1/discord/events",
      applicationIngestionToken: "ingest-secret",
      fetch: async (input: RequestInfo | URL, init?: RequestInit) => {
        payload = await new Request(input, init).json();
        return new Response("{}", { status: 200 });
      },
    } satisfies DiscordbotOptions;

    await ingestObservedDiscordMessages(options, ["M1", "M2"].map((messageId) => ({
      guildId: "G1",
      channelId: "C1",
      messageId,
      authorId: "U1",
      authorName: "member",
      content: messageId,
      createdAt: "2026-08-10T10:00:00.000Z",
      attachments: [],
    })));

    expect(payload?.events).toHaveLength(3);
    expect(payload?.events?.filter((event) => event.type === "member_upsert"))
      .toHaveLength(1);
    expect(payload?.events?.filter((event) => event.type === "message_upsert"))
      .toHaveLength(2);
  });
});
