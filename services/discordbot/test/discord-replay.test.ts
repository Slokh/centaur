import { describe, expect, test } from "bun:test";
import {
  parseDiscordPermalink,
  resolveDiscordReplayContext,
} from "../src/discord-replay";

describe("Discord replay context", () => {
  test("parses Discord permalinks", () => {
    expect(
      parseDiscordPermalink("https://discord.com/channels/1/2/3"),
    ).toEqual({ guildId: "1", channelId: "2", messageId: "3" });
    expect(() => parseDiscordPermalink("https://example.com/1/2/3")).toThrow();
  });

  test("reconstructs a reply chain and resolves member visibility", async () => {
    const messages = new Map([
      ["10", message("10", "member", "first")],
      ["11", message("11", "ai", "answer", "10", true)],
      ["12", message("12", "member", "follow up", "11")],
      ["13", message("13", "ai", "final", "10", true)],
    ]);
    const fetch = async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/channels/2")) return json({ id: "2", type: 0 });
      if (url.includes("/messages?")) return json([...messages.values()].slice(1));
      const id = url.match(/\/messages\/(\d+)$/)?.[1];
      if (id) return json(messages.get(id));
      if (url.endsWith("/guilds/1/members/member")) return json({ roles: [] });
      if (url.endsWith("/guilds/1/roles")) {
        return json([{ id: "1", permissions: String(1n << 10n) }]);
      }
      if (url.endsWith("/guilds/1/channels")) return json([{ id: "2", type: 0 }]);
      return new Response(null, { status: 404 });
    };
    const result = await resolveDiscordReplayContext({
      applicationId: "ai",
      botToken: "secret",
      fetch: fetch as typeof globalThis.fetch,
      reference: "https://discord.com/channels/1/2/13",
    });
    expect(result.source_message_id).toBe("12");
    expect(result.messages.map((item) => [item.role, item.text])).toEqual([
      ["user", "first"],
      ["assistant", "answer"],
      ["user", "follow up"],
    ]);
    expect(result.visible_channel_ids).toEqual(["2"]);
  });

  test("retries Discord rate limits", async () => {
    let attempts = 0;
    const fetch = async (input: RequestInfo | URL) => {
      attempts += 1;
      if (attempts === 1) {
        return Response.json({ retry_after: 0 }, { status: 429 });
      }
      const url = String(input);
      if (url.endsWith("/channels/2/messages/10")) {
        return json(message("10", "member", "hello"));
      }
      if (url.endsWith("/channels/2")) return json({ id: "2", type: 0 });
      if (url.includes("/messages?")) return json([]);
      if (url.endsWith("/guilds/1/members/member")) return json({ roles: [] });
      if (url.endsWith("/guilds/1/roles")) {
        return json([{ id: "1", permissions: String(1n << 10n) }]);
      }
      if (url.endsWith("/guilds/1/channels")) return json([{ id: "2", type: 0 }]);
      return new Response(null, { status: 404 });
    };
    const result = await resolveDiscordReplayContext({
      applicationId: "ai",
      botToken: "secret",
      fetch: fetch as typeof globalThis.fetch,
      reference: "https://discord.com/channels/1/2/10",
    });
    expect(result.source_message_id).toBe("10");
    expect(attempts).toBeGreaterThan(1);
  });
});

function message(
  id: string,
  authorId: string,
  content: string,
  reference?: string,
  bot = false,
) {
  return {
    attachments: [],
    author: { bot, id: authorId, username: authorId },
    channel_id: "2",
    content,
    id,
    ...(reference ? { message_reference: { message_id: reference } } : {}),
    timestamp: "2026-08-13T00:00:00Z",
  };
}

function json(value: unknown): Response {
  return Response.json(value);
}
