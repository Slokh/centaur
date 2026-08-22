import { describe, expect, it } from "bun:test";
import { MemoryDiscordEventSinkOutbox } from "../src/discord-event-sink-outbox";
import {
  discordAttachmentExpiry,
  ingestObservedDiscordMessage,
  ingestObservedDiscordMessages,
  recoverDiscordEventSink,
} from "../src/discord-event-sink";
import type { DiscordbotOptions } from "../src/types";

describe("Discord Discord event sink", () => {
  it("discards successful application response bodies", async () => {
    let cancelled = false;
    const options = {
      apiUrl: "http://centaur",
      applicationId: "app",
      botToken: "bot",
      publicKey: "key",
      eventSinkUrl: "http://application.local/v1/discord/events",
      eventSinkToken: "ingest-secret",
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
      eventSinkUrl: "http://application.local/v1/discord/events",
      eventSinkToken: "ingest-secret",
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
      source: "discord",
      version: 1,
      event: {
        channel_id: "T1",
        thread_id: "T1",
        message_id: "M1",
      },
    });
  });

  it("durably retries an event after the application is unavailable", async () => {
    const outbox = new MemoryDiscordEventSinkOutbox();
    await outbox.connect();
    let available = false;
    let delivered = 0;
    const options = {
      apiUrl: "http://centaur",
      applicationId: "app",
      botToken: "bot",
      publicKey: "key",
      eventSinkUrl: "http://application.local/v1/discord/events",
      eventSinkToken: "ingest-secret",
      eventSinkOutbox: outbox,
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
      outbox,
    );
    expect(delivered).toBe(0);

    expect(await recoverDiscordEventSink(options, outbox)).toMatchObject({
      claimed: 1,
      delivered: 0,
      failed: 1,
    });

    available = true;
    await Bun.sleep(1_010);
    expect(await recoverDiscordEventSink(options, outbox)).toMatchObject({
      claimed: 1,
      delivered: 1,
      failed: 0,
    });
    expect(delivered).toBe(2);
    expect(await recoverDiscordEventSink(options, outbox)).toMatchObject({
      claimed: 0,
    });
    expect(delivered).toBe(2);
  });

  it("does not wait for slow application delivery after writing the outbox", async () => {
    const outbox = new MemoryDiscordEventSinkOutbox();
    await outbox.connect();
    let releaseDelivery: (() => void) | undefined;
    const deliveryBlocked = new Promise<void>((resolve) => {
      releaseDelivery = resolve;
    });
    const options = {
      apiUrl: "http://centaur",
      applicationId: "app",
      botToken: "bot",
      publicKey: "key",
      eventSinkUrl: "http://application.local/v1/discord/events",
      eventSinkToken: "ingest-secret",
      eventSinkOutbox: outbox,
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
      outbox,
    );

    await expect(
      Promise.race([
        ingestion.then(() => "returned"),
        Bun.sleep(100).then(() => "blocked"),
      ]),
    ).resolves.toBe("returned");
    expect(await outbox.pendingCount()).toBe(1);
    releaseDelivery?.();
  });

  it("decodes Discord signed attachment expiry", () => {
    expect(discordAttachmentExpiry("https://cdn.discordapp.com/a?ex=65c00000"))
      .toBe(new Date(Number.parseInt("65c00000", 16) * 1000).toISOString());
    expect(discordAttachmentExpiry("not a url")).toBeNull();
  });

  it("sends one archive batch and deduplicates repeated member events", async () => {
    let payload:
      | { event?: { events?: Record<string, unknown>[] } }
      | undefined;
    const options = {
      apiUrl: "http://centaur",
      applicationId: "app",
      botToken: "bot",
      publicKey: "key",
      eventSinkUrl: "http://application.local/v1/discord/events",
      eventSinkToken: "ingest-secret",
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

    expect(payload?.event?.events).toHaveLength(3);
    expect(payload?.event?.events?.filter((event) => event.type === "member_upsert"))
      .toHaveLength(1);
    expect(payload?.event?.events?.filter((event) => event.type === "message_upsert"))
      .toHaveLength(2);
  });

  it("bounds each recovery claim even with a large pending backlog", async () => {
    const outbox = new MemoryDiscordEventSinkOutbox();
    await outbox.connect();
    for (let index = 0; index < 10_000; index += 1) {
      await outbox.enqueue(`message:${index}`, {
        source_key: `message:${index}`,
        type: "message_upsert",
      });
    }
    let delivered = 0;
    const options = {
      apiUrl: "http://centaur",
      applicationId: "app",
      eventSinkOutbox: outbox,
      eventSinkRecoveryBatchSize: 25,
      eventSinkToken: "ingest-secret",
      eventSinkUrl: "http://application.local/v1/discord/events",
      botToken: "bot",
      fetch: async () => {
        delivered += 1;
        return new Response(null, { status: 204 });
      },
      publicKey: "key",
    } satisfies DiscordbotOptions;

    expect(await recoverDiscordEventSink(options, outbox)).toEqual({
      claimed: 25,
      delivered: 25,
      failed: 0,
    });
    expect(delivered).toBe(25);
    expect(await outbox.pendingCount()).toBe(9_975);
  });

  it("prevents a stale lease holder from acknowledging reclaimed work", async () => {
    const outbox = new MemoryDiscordEventSinkOutbox();
    await outbox.enqueue("message:lease", { source_key: "message:lease" });
    const [first] = await outbox.claim(1, 5);
    expect(first).toBeDefined();
    expect(await outbox.claim(1, 5)).toHaveLength(0);
    await Bun.sleep(10);
    const [second] = await outbox.claim(1, 5);
    expect(second?.leaseToken).not.toBe(first?.leaseToken);
    expect(await outbox.acknowledge(first!)).toBeFalse();
    expect(await outbox.acknowledge(second!)).toBeTrue();
    expect(await outbox.pendingCount()).toBe(0);
  });

  it("dead-letters a poison event after bounded delivery attempts", async () => {
    const outbox = new MemoryDiscordEventSinkOutbox();
    await outbox.enqueue("message:poison", { source_key: "message:poison" });
    for (let attempt = 0; attempt < 12; attempt += 1) {
      const [claim] = await outbox.claim(1, 1);
      expect(claim).toBeDefined();
      await outbox.retry(claim!, 0, "permanent rejection");
    }
    expect(await outbox.pendingCount()).toBe(0);
    expect(await outbox.deadLetterCount()).toBe(1);
    expect(await outbox.claim(1, 1)).toEqual([]);
  });
});
