import { describe, expect, it } from "bun:test";
import { resolveDiscordVisibleChannelIds } from "../src/discord-visibility";

const VIEW_CHANNEL = String(1 << 10);

describe("Discord visibility authority", () => {
  it("applies everyone, aggregate role, then member overwrites", async () => {
    const fetch = async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/roles")) {
        return Response.json([
          { id: "G1", permissions: VIEW_CHANNEL },
          { id: "R1", permissions: "0" },
        ]);
      }
      return Response.json([
        { id: "open", type: 0 },
        {
          id: "denied",
          type: 0,
          permission_overwrites: [
            { id: "G1", type: 0, deny: VIEW_CHANNEL },
          ],
        },
        {
          id: "member-restored",
          type: 0,
          permission_overwrites: [
            { id: "G1", type: 0, deny: VIEW_CHANNEL },
            { id: "U1", type: 1, allow: VIEW_CHANNEL },
          ],
        },
      ]);
    };

    const visible = await resolveDiscordVisibleChannelIds({
      botToken: "token",
      currentChannelId: "current",
      currentThreadId: "thread",
      guildId: "G1",
      rawMessage: { member: { roles: ["R1"] } },
      userId: "U1",
      fetch: fetch as typeof globalThis.fetch,
    });

    expect(visible).toEqual(["current", "member-restored", "open", "thread"]);
  });

  it("fails closed when member roles are absent", async () => {
    await expect(
      resolveDiscordVisibleChannelIds({
        botToken: "token",
        currentChannelId: "current",
        guildId: "G1",
        rawMessage: {},
        userId: "U1",
      }),
    ).rejects.toThrow("missing member roles");
  });
});
