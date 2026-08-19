import { describe, expect, it } from "bun:test";
import { createMemoryState } from "@chat-adapter/state-memory";
import { reconcileDiscordArchive } from "../src/discord-reconciliation";
import type { DiscordbotOptions } from "../src/types";

describe("Discord archive reconciliation", () => {
  it("discards failed Discord response bodies", async () => {
    let cancelled = false;
    const state = createMemoryState();
    await state.connect();
    const options = {
      apiUrl: "http://centaur",
      applicationId: "app",
      botToken: "bot",
      publicKey: "key",
      discordApiUrl: "http://discord",
      guildAllowlist: ["1"],
      applicationIngestionUrl: "http://application.local/v1/discord/events",
      applicationIngestionToken: "secret",
      fetch: async () => new Response(new ReadableStream({
        cancel() {
          cancelled = true;
        },
      }), { status: 503 }),
    } satisfies DiscordbotOptions;

    await expect(reconcileDiscordArchive(options, state)).rejects.toThrow(
      "Discord reconciliation returned 503",
    );
    expect(cancelled).toBeTrue();
  });

  it("backfills messages and advances a per-channel checkpoint", async () => {
    const state = createMemoryState();
    await state.connect();
    const delivered: Record<string, unknown>[] = [];
    const discordRequests: string[] = [];
    const options = {
      apiUrl: "http://centaur",
      applicationId: "app",
      botToken: "bot",
      publicKey: "key",
      discordApiUrl: "http://discord",
      guildAllowlist: ["1"],
      applicationIngestionUrl: "http://application.local/v1/discord/events",
      applicationIngestionToken: "secret",
      fetch: async (input: RequestInfo | URL, init?: RequestInit) => {
        const request = new Request(input, init);
        if (request.url.startsWith("http://application.local")) {
          recordDelivery(delivered, await request.json());
          return new Response("{}", { status: 200 });
        }
        discordRequests.push(request.url);
        if (request.url.endsWith("/guilds/1/channels")) {
          return Response.json([{ id: "10", type: 0, name: "general" }]);
        }
        if (request.url.endsWith("/guilds/1/threads/active")) {
          return Response.json({ threads: [] });
        }
        if (request.url.includes("/channels/10/threads/archived/public")) {
          return Response.json({ threads: [], has_more: false });
        }
        if (request.url.includes("/channels/10/messages") && !request.url.includes("after=")) {
          return Response.json([{
            id: "20",
            channel_id: "10",
            content: "release notes",
            timestamp: "2026-08-10T10:00:00.000Z",
            author: { id: "30", username: "member" },
            embeds: [{ title: "v1" }],
            attachments: [],
          }]);
        }
        return Response.json([]);
      },
    } satisfies DiscordbotOptions;

    expect(await reconcileDiscordArchive(options, state)).toBe(1);
    expect(delivered.some((event) => event.type === "channel_upsert")).toBeTrue();
    expect(delivered).toContainEqual(expect.objectContaining({
      type: "message_upsert",
      message_id: "20",
      normalized_payload: expect.objectContaining({ embeds: [{ title: "v1" }] }),
    }));

    expect(await reconcileDiscordArchive(options, state)).toBe(0);
    expect(discordRequests.some((url) => url.includes("after=20"))).toBeTrue();
  });

  it("continues historical pagination behind the initial latest page", async () => {
    const state = createMemoryState();
    await state.connect();
    const delivered: Record<string, unknown>[] = [];
    const discordRequests: string[] = [];
    const latest = Array.from({ length: 100 }, (_, index) => message(String(200 + index)));
    const older = [message("199"), message("198")];
    const options = {
      apiUrl: "http://centaur",
      applicationId: "app",
      botToken: "bot",
      publicKey: "key",
      discordApiUrl: "http://discord",
      guildAllowlist: ["1"],
      applicationIngestionUrl: "http://application.local/v1/discord/events",
      applicationIngestionToken: "secret",
      fetch: async (input: RequestInfo | URL, init?: RequestInit) => {
        const request = new Request(input, init);
        if (request.url.startsWith("http://application.local")) {
          recordDelivery(delivered, await request.json());
          return new Response("{}", { status: 200 });
        }
        discordRequests.push(request.url);
        if (request.url.endsWith("/guilds/1/channels")) {
          return Response.json([{ id: "10", type: 0, name: "general" }]);
        }
        if (request.url.endsWith("/guilds/1/threads/active")) {
          return Response.json({ threads: [] });
        }
        if (request.url.includes("/channels/10/threads/archived/public")) {
          return Response.json({ threads: [], has_more: false });
        }
        if (request.url.includes("after=299")) return Response.json([]);
        if (request.url.includes("before=200")) return Response.json(older);
        if (request.url.includes("/channels/10/messages")) return Response.json(latest);
        return Response.json([]);
      },
    } satisfies DiscordbotOptions;

    expect(await reconcileDiscordArchive(options, state)).toBe(102);
    expect(discordRequests.some((url) => url.includes("before=200"))).toBeTrue();
    expect(delivered).toContainEqual(expect.objectContaining({ message_id: "198" }));

    expect(await reconcileDiscordArchive(options, state)).toBe(0);
    expect(discordRequests.filter((url) => url.includes("before=200"))).toHaveLength(1);
  });

  it("discovers archived public threads and backfills their messages once", async () => {
    const state = createMemoryState();
    await state.connect();
    const delivered: Record<string, unknown>[] = [];
    const options = {
      apiUrl: "http://centaur",
      applicationId: "app",
      botToken: "bot",
      publicKey: "key",
      discordApiUrl: "http://discord",
      guildAllowlist: ["1"],
      applicationIngestionUrl: "http://application.local/v1/discord/events",
      applicationIngestionToken: "secret",
      fetch: async (input: RequestInfo | URL, init?: RequestInit) => {
        const request = new Request(input, init);
        if (request.url.startsWith("http://application.local")) {
          recordDelivery(delivered, await request.json());
          return new Response("{}", { status: 200 });
        }
        if (request.url.endsWith("/guilds/1/channels")) {
          return Response.json([{ id: "10", type: 0, name: "general" }]);
        }
        if (request.url.endsWith("/guilds/1/threads/active")) {
          return Response.json({ threads: [] });
        }
        if (request.url.includes("/channels/10/threads/archived/public")) {
          return Response.json({
            threads: [{
              id: "50",
              type: 11,
              name: "old topic",
              parent_id: "10",
              thread_metadata: { archive_timestamp: "2026-08-01T00:00:00.000Z" },
            }],
            has_more: false,
          });
        }
        if (request.url.includes("/channels/50/messages") && !request.url.includes("after=")) {
          return Response.json([{ ...message("60"), channel_id: "50" }]);
        }
        return Response.json([]);
      },
    } satisfies DiscordbotOptions;

    expect(await reconcileDiscordArchive(options, state)).toBe(1);
    expect(delivered).toContainEqual(expect.objectContaining({
      type: "channel_upsert",
      channel_id: "50",
      parent_id: "10",
    }));
    expect(delivered).toContainEqual(expect.objectContaining({
      type: "message_upsert",
      channel_id: "50",
      thread_id: "50",
      message_id: "60",
    }));

    expect(await reconcileDiscordArchive(options, state)).toBe(0);
  });

  it("does not advance an archived-thread cursor past failed reconciliation", async () => {
    const state = createMemoryState();
    await state.connect();
    const delivered: Record<string, unknown>[] = [];
    let failArchivedMessages = true;
    let archivedDiscoveryRequests = 0;
    const options = {
      apiUrl: "http://centaur",
      applicationId: "app",
      botToken: "bot",
      publicKey: "key",
      discordApiUrl: "http://discord",
      guildAllowlist: ["1"],
      applicationIngestionUrl: "http://application.local/v1/discord/events",
      applicationIngestionToken: "secret",
      fetch: async (input: RequestInfo | URL, init?: RequestInit) => {
        const request = new Request(input, init);
        if (request.url.startsWith("http://application.local")) {
          recordDelivery(delivered, await request.json());
          return new Response("{}", { status: 200 });
        }
        if (request.url.endsWith("/guilds/1/channels")) {
          return Response.json([{ id: "10", type: 0, name: "general" }]);
        }
        if (request.url.endsWith("/guilds/1/threads/active")) {
          return Response.json({ threads: [] });
        }
        if (request.url.includes("/channels/10/threads/archived/public")) {
          archivedDiscoveryRequests += 1;
          return Response.json({
            threads: [{
              id: "50",
              type: 11,
              name: "recoverable topic",
              parent_id: "10",
              thread_metadata: { archive_timestamp: "2026-08-01T00:00:00.000Z" },
            }],
            has_more: false,
          });
        }
        if (request.url.includes("/channels/50/messages")) {
          if (failArchivedMessages) {
            failArchivedMessages = false;
            return new Response("temporary failure", { status: 503 });
          }
          if (request.url.includes("after=")) return Response.json([]);
          return Response.json([{ ...message("60"), channel_id: "50" }]);
        }
        return Response.json([]);
      },
    } satisfies DiscordbotOptions;

    await expect(reconcileDiscordArchive(options, state)).rejects.toThrow(
      "Discord reconciliation returned 503",
    );
    expect(await reconcileDiscordArchive(options, state)).toBe(1);
    expect(archivedDiscoveryRequests).toBe(2);
    expect(delivered).toContainEqual(
      expect.objectContaining({ type: "message_upsert", message_id: "60" }),
    );
  });

  it("does not advance a channel checkpoint past failed batch delivery", async () => {
    const state = createMemoryState();
    await state.connect();
    let failBatch = true;
    let latestPageRequests = 0;
    const options = {
      apiUrl: "http://centaur",
      applicationId: "app",
      botToken: "bot",
      publicKey: "key",
      discordApiUrl: "http://discord",
      guildAllowlist: ["1"],
      applicationIngestionUrl: "http://application.local/v1/discord/events",
      applicationIngestionToken: "secret",
      fetch: async (input: RequestInfo | URL, init?: RequestInit) => {
        const request = new Request(input, init);
        if (request.url.startsWith("http://application.local")) {
          const payload = await request.json() as { events?: unknown[] };
          if (payload.events && failBatch) {
            failBatch = false;
            return new Response("temporary failure", { status: 503 });
          }
          return new Response("{}", { status: 200 });
        }
        if (request.url.endsWith("/guilds/1/channels")) {
          return Response.json([{ id: "10", type: 0, name: "general" }]);
        }
        if (request.url.endsWith("/guilds/1/threads/active")) {
          return Response.json({ threads: [] });
        }
        if (request.url.includes("/channels/10/threads/archived/public")) {
          return Response.json({ threads: [], has_more: false });
        }
        if (request.url.includes("/channels/10/messages")) {
          if (request.url.includes("after=")) return Response.json([]);
          latestPageRequests += 1;
          return Response.json([message("20")]);
        }
        return Response.json([]);
      },
    } satisfies DiscordbotOptions;

    await expect(reconcileDiscordArchive(options, state)).rejects.toThrow(
      "application ingestion returned 503",
    );
    expect(await reconcileDiscordArchive(options, state)).toBe(1);
    expect(latestPageRequests).toBe(2);
  });

  it("skips inaccessible channels without interrupting other channel backfills", async () => {
    const state = createMemoryState();
    await state.connect();
    const delivered: Record<string, unknown>[] = [];
    const warnings: Array<{ message: string; data?: unknown }> = [];
    const logger = {
      debug: () => undefined,
      info: () => undefined,
      warn: (message: string, data?: unknown) => {
        warnings.push({ message, data });
      },
      error: () => undefined,
      child: () => logger,
    };
    const options = {
      apiUrl: "http://centaur",
      applicationId: "app",
      botToken: "bot",
      publicKey: "key",
      discordApiUrl: "http://discord",
      guildAllowlist: ["1"],
      applicationIngestionUrl: "http://application.local/v1/discord/events",
      applicationIngestionToken: "secret",
      logger,
      fetch: async (input: RequestInfo | URL, init?: RequestInit) => {
        const request = new Request(input, init);
        if (request.url.startsWith("http://application.local")) {
          recordDelivery(delivered, await request.json());
          return new Response("{}", { status: 200 });
        }
        if (request.url.endsWith("/guilds/1/channels")) {
          return Response.json([
            { id: "10", type: 0, name: "private" },
            { id: "11", type: 0, name: "general" },
          ]);
        }
        if (request.url.endsWith("/guilds/1/threads/active")) {
          return Response.json({ threads: [] });
        }
        if (request.url.includes("/threads/archived/public")) {
          return Response.json({ threads: [], has_more: false });
        }
        if (request.url.includes("/channels/10/messages")) {
          return new Response("missing access", { status: 403 });
        }
        if (request.url.includes("/channels/11/messages") && !request.url.includes("after=")) {
          return Response.json([{ ...message("21"), channel_id: "11" }]);
        }
        return Response.json([]);
      },
    } satisfies DiscordbotOptions;

    expect(await reconcileDiscordArchive(options, state)).toBe(1);
    expect(delivered).toContainEqual(
      expect.objectContaining({ type: "message_upsert", message_id: "21" }),
    );
    expect(warnings).toContainEqual(expect.objectContaining({
      message: "discordbot_archive_channel_skipped",
      data: { channel_id: "10", status: 403 },
    }));
  });

  it("skips inaccessible archived-thread endpoints and continues with later parents", async () => {
    const state = createMemoryState();
    await state.connect();
    const delivered: Record<string, unknown>[] = [];
    const warnings: Array<{ message: string; data?: unknown }> = [];
    const logger = {
      debug: () => undefined,
      info: () => undefined,
      warn: (message: string, data?: unknown) => {
        warnings.push({ message, data });
      },
      error: () => undefined,
      child: () => logger,
    };
    const options = {
      apiUrl: "http://centaur",
      applicationId: "app",
      botToken: "bot",
      publicKey: "key",
      discordApiUrl: "http://discord",
      guildAllowlist: ["1"],
      applicationIngestionUrl: "http://application.local/v1/discord/events",
      applicationIngestionToken: "secret",
      logger,
      fetch: async (input: RequestInfo | URL, init?: RequestInit) => {
        const request = new Request(input, init);
        if (request.url.startsWith("http://application.local")) {
          recordDelivery(delivered, await request.json());
          return new Response("{}", { status: 200 });
        }
        if (request.url.endsWith("/guilds/1/channels")) {
          return Response.json([
            { id: "10", type: 0, name: "private" },
            { id: "11", type: 0, name: "general" },
          ]);
        }
        if (request.url.endsWith("/guilds/1/threads/active")) {
          return Response.json({ threads: [] });
        }
        if (request.url.includes("/channels/10/threads/archived/public")) {
          return new Response("missing access", { status: 403 });
        }
        if (request.url.includes("/channels/11/threads/archived/public")) {
          return Response.json({
            threads: [{
              id: "50",
              type: 11,
              name: "old topic",
              parent_id: "11",
              thread_metadata: { archive_timestamp: "2026-08-01T00:00:00.000Z" },
            }],
            has_more: false,
          });
        }
        if (request.url.includes("/channels/50/messages") && !request.url.includes("after=")) {
          return Response.json([{ ...message("60"), channel_id: "50" }]);
        }
        return Response.json([]);
      },
    } satisfies DiscordbotOptions;

    expect(await reconcileDiscordArchive(options, state)).toBe(1);
    expect(delivered).toContainEqual(
      expect.objectContaining({ type: "message_upsert", message_id: "60" }),
    );
    expect(warnings).toContainEqual(expect.objectContaining({
      message: "discordbot_archive_channel_skipped",
      data: { channel_id: "10", status: 403 },
    }));
  });
});

function message(id: string) {
  return {
    id,
    channel_id: "10",
    content: `message ${id}`,
    timestamp: "2026-08-10T10:00:00.000Z",
    author: { id: "30", username: "member" },
    attachments: [],
  };
}

function recordDelivery(
  delivered: Record<string, unknown>[],
  payload: unknown,
): void {
  const candidate = payload as { events?: Record<string, unknown>[] };
  if (Array.isArray(candidate.events)) {
    delivered.push(...candidate.events);
  } else {
    delivered.push(payload as Record<string, unknown>);
  }
}
