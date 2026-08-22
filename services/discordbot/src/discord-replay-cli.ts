import { resolveDiscordReplayContext } from "./discord-replay";

if (import.meta.main) {
  const reference = Bun.argv[2];
  const userId = Bun.argv[3];
  const botToken = process.env.DISCORD_BOT_TOKEN;
  const applicationId = process.env.DISCORD_APPLICATION_ID;
  if (!reference || !botToken || !applicationId) {
    console.error(
      "usage: DISCORD_BOT_TOKEN=... DISCORD_APPLICATION_ID=... bun src/discord-replay-cli.ts <permalink> [user-id]",
    );
    process.exit(2);
  }
  const context = await resolveDiscordReplayContext({
    applicationId,
    botToken,
    reference,
    ...(userId ? { userId } : {}),
  });
  process.stdout.write(`${JSON.stringify(context)}\n`);
}
