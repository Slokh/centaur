import { DEFAULT_DISCORD_API_URL } from "./discord-threading";

const VIEW_CHANNEL = 1n << 10n;
const READ_MESSAGE_HISTORY = 1n << 16n;
const ADMINISTRATOR = 1n << 3n;
const HISTORY_PERMISSIONS = VIEW_CHANNEL | READ_MESSAGE_HISTORY;
const MESSAGE_CHANNEL_TYPES = new Set([0, 5, 15, 16]);

type DiscordRole = { id: string; permissions?: string };
type DiscordMember = { roles?: string[] };
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
  userId: string;
  fetch?: typeof fetch;
}): Promise<string[]> {
  const apiBase = (input.apiUrl ?? DEFAULT_DISCORD_API_URL).replace(/\/$/, "");
  const fetchFn = input.fetch ?? fetch;
  const headers = { authorization: `Bot ${input.botToken}` };
  const [memberResponse, rolesResponse, channelsResponse] = await Promise.all([
    fetchFn(`${apiBase}/guilds/${input.guildId}/members/${input.userId}`, {
      headers,
    }),
    fetchFn(`${apiBase}/guilds/${input.guildId}/roles`, { headers }),
    fetchFn(`${apiBase}/guilds/${input.guildId}/channels`, { headers }),
  ]);
  if (!memberResponse.ok || !rolesResponse.ok || !channelsResponse.ok) {
    throw new Error(
      `Discord visibility lookup failed (member=${memberResponse.status}, roles=${rolesResponse.status}, channels=${channelsResponse.status})`,
    );
  }
  const member = (await memberResponse.json()) as DiscordMember;
  const roles = (await rolesResponse.json()) as DiscordRole[];
  const channels = (await channelsResponse.json()) as DiscordChannel[];
  if (
    !Array.isArray(member.roles) ||
    member.roles.some((role) => typeof role !== "string") ||
    !Array.isArray(roles) ||
    !Array.isArray(channels)
  ) {
    throw new Error("Discord visibility lookup returned invalid payloads");
  }

  const effectiveRoleIds = new Set([input.guildId, ...member.roles]);
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
        canReadChannelHistory(
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

  // Active threads are absent from the guild channel catalog. Inherit the
  // parent channel's historical-read authority only after it was validated;
  // receiving one live event does not itself authorize historical retrieval.
  if (input.currentThreadId && visible.has(input.currentChannelId)) {
    visible.add(input.currentThreadId);
  }
  return [...visible].sort();
}

function canReadChannelHistory(
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
  return (permissions & HISTORY_PERMISSIONS) === HISTORY_PERMISSIONS;
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
