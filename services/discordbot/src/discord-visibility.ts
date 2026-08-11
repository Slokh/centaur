import { DEFAULT_DISCORD_API_URL } from "./discord-threading";

const VIEW_CHANNEL = 1n << 10n;
const ADMINISTRATOR = 1n << 3n;
const MESSAGE_CHANNEL_TYPES = new Set([0, 5, 15, 16]);

type DiscordRole = { id: string; permissions?: string };
type DiscordOverwrite = {
  id: string;
  type: number;
  allow?: string;
  deny?: string;
};
type DiscordChannel = {
  id: string;
  type: number;
  permission_overwrites?: DiscordOverwrite[];
};

/**
 * Resolve the message channels a Discord member may currently view. The
 * resulting ids are authority input for downstream retrieval, never a model
 * hint. Any API or payload ambiguity throws so callers can fail closed to the
 * current conversation only.
 */
export async function resolveDiscordVisibleChannelIds(input: {
  apiUrl?: string;
  botToken: string;
  currentChannelId: string;
  currentThreadId?: string;
  guildId: string;
  rawMessage: unknown;
  userId: string;
  fetch?: typeof fetch;
}): Promise<string[]> {
  const memberRoleIds = discordMemberRoleIds(input.rawMessage);
  const apiBase = (input.apiUrl ?? DEFAULT_DISCORD_API_URL).replace(/\/$/, "");
  const fetchFn = input.fetch ?? fetch;
  const headers = { authorization: `Bot ${input.botToken}` };
  const [rolesResponse, channelsResponse] = await Promise.all([
    fetchFn(`${apiBase}/guilds/${input.guildId}/roles`, { headers }),
    fetchFn(`${apiBase}/guilds/${input.guildId}/channels`, { headers }),
  ]);
  if (!rolesResponse.ok || !channelsResponse.ok) {
    throw new Error(
      `Discord visibility lookup failed (roles=${rolesResponse.status}, channels=${channelsResponse.status})`,
    );
  }
  const roles = (await rolesResponse.json()) as DiscordRole[];
  const channels = (await channelsResponse.json()) as DiscordChannel[];
  if (!Array.isArray(roles) || !Array.isArray(channels)) {
    throw new Error("Discord visibility lookup returned invalid payloads");
  }

  const effectiveRoleIds = new Set([input.guildId, ...memberRoleIds]);
  let basePermissions = 0n;
  for (const role of roles) {
    if (effectiveRoleIds.has(role.id)) {
      basePermissions |= permissionBits(role.permissions);
    }
  }

  const visible = new Set<string>();
  if ((basePermissions & ADMINISTRATOR) === ADMINISTRATOR) {
    for (const channel of channels) {
      if (MESSAGE_CHANNEL_TYPES.has(channel.type)) visible.add(channel.id);
    }
  } else {
    for (const channel of channels) {
      if (
        MESSAGE_CHANNEL_TYPES.has(channel.type) &&
        canViewChannel(
          basePermissions,
          channel.permission_overwrites ?? [],
          input.guildId,
          effectiveRoleIds,
          input.userId,
        )
      ) {
        visible.add(channel.id);
      }
    }
  }

  // The triggering conversation was already admitted by Discord and ingress;
  // retain it even if the guild channel catalog omits active thread objects.
  visible.add(input.currentChannelId);
  if (input.currentThreadId) visible.add(input.currentThreadId);
  return [...visible].sort();
}

function canViewChannel(
  base: bigint,
  overwrites: DiscordOverwrite[],
  guildId: string,
  roleIds: Set<string>,
  userId: string,
): boolean {
  let permissions = base;
  const everyone = overwrites.find(
    (overwrite) => overwrite.type === 0 && overwrite.id === guildId,
  );
  permissions = applyOverwrite(permissions, everyone);

  let roleAllow = 0n;
  let roleDeny = 0n;
  for (const overwrite of overwrites) {
    if (overwrite.type === 0 && roleIds.has(overwrite.id)) {
      roleAllow |= permissionBits(overwrite.allow);
      roleDeny |= permissionBits(overwrite.deny);
    }
  }
  permissions = (permissions & ~roleDeny) | roleAllow;
  permissions = applyOverwrite(
    permissions,
    overwrites.find(
      (overwrite) => overwrite.type === 1 && overwrite.id === userId,
    ),
  );
  return (permissions & VIEW_CHANNEL) === VIEW_CHANNEL;
}

function applyOverwrite(
  permissions: bigint,
  overwrite: DiscordOverwrite | undefined,
): bigint {
  if (!overwrite) return permissions;
  return (
    (permissions & ~permissionBits(overwrite.deny)) |
    permissionBits(overwrite.allow)
  );
}

function permissionBits(value: string | undefined): bigint {
  if (!value) return 0n;
  try {
    return BigInt(value);
  } catch {
    throw new Error("Discord permission value is not an integer");
  }
}

function discordMemberRoleIds(rawMessage: unknown): string[] {
  if (!rawMessage || typeof rawMessage !== "object") {
    throw new Error("Discord message is missing member roles");
  }
  const member = (rawMessage as { member?: unknown }).member;
  if (!member || typeof member !== "object") {
    throw new Error("Discord message is missing member roles");
  }
  const roles = (member as { roles?: unknown }).roles;
  if (!Array.isArray(roles) || roles.some((role) => typeof role !== "string")) {
    throw new Error("Discord message member roles are invalid");
  }
  return roles;
}
