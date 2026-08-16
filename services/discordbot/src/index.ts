import { randomUUID } from "node:crypto";
import {
  harnessToChatSdkStream,
  type ChatSDKStreamChunk,
  type CodexAppServerToChatStreamOptions,
  type RendererEvent,
} from "@centaur/rendering";
import { createDiscordAdapter } from "@chat-adapter/discord";
import { createPostgresState } from "@chat-adapter/state-pg";
import {
  Chat,
  type Adapter,
  type Logger,
  type Message as ChatMessage,
  type MessageContext,
  type StateAdapter,
  type Thread,
} from "chat";
import { Hono } from "hono";
import pg from "pg";
import {
  discordTurnDeliveryKey,
  isAllowedDiscordGuild,
  isAllowedDiscordMessage,
  isGuildAllowlistEmpty,
  parseDiscordThreadKey,
  resolveTriggerBotAllowlist,
} from "./discord-allowlist";
import { DiscordNarrator, reactToDiscordMessage } from "./discord-narrator";
import {
  ingestDeletedDiscordMessage,
  ingestObservedDiscordChannel,
  ingestObservedDiscordMessage,
  recoverApplicationIngestion,
} from "./discord-ingestion";
import { fetchThreadStarterMessage } from "./discord-starter";
import { reconcileDiscordArchive } from "./discord-reconciliation";
import {
  deriveThreadName,
  fetchDiscordChannelName,
  isThreadCreatedForMessage,
  renameThreadFromMessage,
} from "./discord-threading";
import { setGatewayConnected } from "./gateway";
import {
  collectInitialContext,
  executeSessionTurn,
  forwardToSessionApi,
  isContentlessApiMessage,
  isDiscordPermissionError,
  isRetryableSessionApiError,
  openSessionEventStream,
  serializeMessage,
  sessionStreamError,
  startingStreamNotification,
} from "./session-api";
import type {
  Discordbot,
  DiscordbotApiMessage,
  DiscordbotExecuteSessionResponse,
  DiscordbotMessageMode,
  DiscordbotOptions,
  DiscordbotRenderObligation,
  DiscordbotRendererSource,
  DiscordbotThreadState,
  DiscordbotTrace,
  ForwardSessionInput,
  TypingCapableAdapter,
} from "./types";
import {
  elapsedMs,
  errorMessage,
  GuildExecutionLimiter,
  noopLogger,
  nowMs,
  singleFlight,
  sliceSurrogateSafe,
  suppressDiscordLinkEmbeds,
  takeDiscordMessageChunk,
  traceLog,
} from "./utils";

export type {
  Discordbot,
  DiscordbotApiAttachment,
  DiscordbotApiAuthor,
  DiscordbotApiMessage,
  DiscordbotAppendMessagesRequest,
  DiscordbotCreateSessionRequest,
  DiscordbotExecuteSessionRequest,
  DiscordbotExecuteSessionResponse,
  DiscordbotFetch,
  DiscordbotOptions,
  DiscordbotSessionMessage,
  DiscordbotSessionMessageRole,
} from "./types";

const TYPING_KEEPALIVE_MS = 8000;
const RENDER_OBLIGATION_INDEX_KEY = "discordbot:render:index";
const RENDER_OBLIGATION_INDEX_MAX_LENGTH = 2000;
const RENDER_INDEX_TTL_MS = 30 * 24 * 60 * 60 * 1000;
const RENDER_RECOVERY_LEASE_TTL_MS = 2 * 60 * 1000;
const RENDER_LEASE_REFRESH_INTERVAL_MS = 60 * 1000;
const RENDER_RETRY_INITIAL_DELAY_MS = 250;
const RENDER_RETRY_MAX_DELAY_MS = 5_000;
// Discord caps message content at 2000 chars; leave headroom so the honest
// "[truncated ...]" suffix lands instead of the adapter's silent "..." cut.
// Link suppression adds two characters per URL. Keeping the raw chunk at 1500
// guarantees even URL-dense output remains below Discord's hard 2000-char cap
// after every destination is wrapped in `<...>`.
const DISCORD_FALLBACK_TEXT_MAX_CHARS = 1_500;
const POSTGRES_CONNECT_INITIAL_DELAY_MS = 250;
const POSTGRES_CONNECT_MAX_DELAY_MS = 10_000;
// Discord delta (no slackbotv2 analog): `activeExecution` persisted before the
// execution commit is only cleared by the render finally; a crash/SIGTERM in
// between would wedge the thread forever (Gateway ingress has no redelivery),
// so the flag is ignored once older than this TTL.
const ACTIVE_EXECUTION_TTL_MS = 30 * 60 * 1000;
// Discord delta (no slackbotv2 analog): cap on the render retry loop. Upstream
// retries forever — tolerable on Slack, where webhook redelivery bounds a
// stuck turn — but here an unbounded loop replays the render from the original
// afterEventId forever (duplicating posts and never settling 👀) whenever the
// error keeps classifying retryable, including a TypeError thrown by a
// programming bug. TypeError stays retryable for parity with upstream (fetch
// network failures surface as TypeErrors); the cap is what kills the loop.
const RENDER_RETRY_MAX_ATTEMPTS = 10;
// Discord delta (no slackbotv2 analog): bounded retry for the synchronous
// create/append phase — Slack relies on webhook redelivery (503) for transient
// failures there; the Gateway has no redelivery and the message is already
// dedupe-marked, so giving up immediately loses the turn permanently.
const FORWARD_RETRY_DELAYS_MS = [1_000, 3_000];
// Discord delta (no slackbotv2 analog): per-guild in-flight execution cap.
const DEFAULT_MAX_CONCURRENT_EXECUTIONS_PER_GUILD = 3;
// Discord delta: final answers are buffered until the renderer completes,
// then posted without edits. Everything past the full-message cap lands in
// one final honestly-truncated message.
const ANSWER_MESSAGE_MAX_CHARS = DISCORD_FALLBACK_TEXT_MAX_CHARS;
const ANSWER_MAX_FULL_MESSAGES = 10;

// The resolved channel name becomes the session principal's display name in
// iron-control (see api-rs derive_principal). api-rs re-upserts the principal on
// every create, so the name must ride every create to stay stable — cache the
// per-channel lookup (mirrors slackbotv2's conversations.info cache). Misses
// expire sooner so a transient channel-fetch failure self-heals.
const CONVERSATION_NAME_CACHE_SUCCESS_TTL_MS = 6 * 60 * 60 * 1000;
const CONVERSATION_NAME_CACHE_MISS_TTL_MS = 10 * 60 * 1000;
type ConversationNameCacheEntry = {
  expiresAtMs: number;
  name: string | undefined;
};
const conversationNameCache = new Map<string, ConversationNameCacheEntry>();

export function clearConversationNameCacheForTests(): void {
  conversationNameCache.clear();
}

/**
 * Resolve the Discord channel name for a thread key, used to name the session
 * principal. Cached per channel and never throws — the name is cosmetic, so a
 * fetch failure just falls back to the synthetic id-based principal name in
 * api-rs.
 */
export async function resolveDiscordConversationName(
  options: DiscordbotOptions,
  threadKey: string,
  logger: Logger,
): Promise<string | undefined> {
  const { channelId } = parseDiscordThreadKey(threadKey);
  if (!channelId) return undefined;
  const cached = conversationNameCache.get(channelId);
  if (cached && cached.expiresAtMs > Date.now()) return cached.name;

  const name = await fetchDiscordChannelName(options, channelId, logger);
  conversationNameCache.set(channelId, {
    expiresAtMs:
      Date.now() +
      (name
        ? CONVERSATION_NAME_CACHE_SUCCESS_TTL_MS
        : CONVERSATION_NAME_CACHE_MISS_TTL_MS),
    name,
  });
  return name;
}

async function resolveVisibleChannelScope(
  options: DiscordbotOptions,
  message: DiscordbotApiMessage,
  threadKey: string,
  logger: Logger,
): Promise<string[]> {
  const { guildId, channelId, threadId } = parseDiscordThreadKey(threadKey);
  const fallback = [channelId, threadId].filter(
    (value): value is string => Boolean(value),
  );
  if (!guildId || !channelId || !options.resolveVisibleChannelIds) return fallback;
  try {
    return await options.resolveVisibleChannelIds({
      currentChannelId: channelId,
      ...(threadId ? { currentThreadId: threadId } : {}),
      guildId,
      userId: message.author.userId,
    });
  } catch (error) {
    logger.warn("discordbot_visibility_scope_failed", {
      channel_id: channelId,
      error: errorMessage(error),
      guild_id: guildId,
      user_id: message.author.userId,
    });
    return fallback;
  }
}

export function createDiscordbot(options: DiscordbotOptions): Discordbot {
  const userName = options.userName ?? "centaur";
  const logger = options.logger ?? noopLogger;

  if (isGuildAllowlistEmpty(options)) {
    logger.warn("discordbot_guild_allowlist_empty_inert", {
      hint: "Set DISCORDBOT_GUILD_ALLOWLIST; the bot ignores all messages until configured.",
    });
  }

  const state = options.state ?? createDefaultState(options, logger);
  const discord = createDiscordAdapter({
    apiUrl: options.discordApiUrl,
    applicationId: options.applicationId,
    botToken: options.botToken,
    conversationMode: options.conversationMode,
    onMessageObserved: async (message) => {
      if (!isAllowedDiscordGuild(message.guildId, options)) return;
      // Observation is a durable background concern, not part of Discord's
      // acknowledgement path. Even the outbox's state writes may involve a
      // remote database and must not delay mention routing, typing, or 👀.
      backgroundWaitUntil(ingestObservedDiscordMessage(options, message, state));
    },
    onMessageDeleted: async (message) => {
      if (!isAllowedDiscordGuild(message.guildId, options)) return;
      backgroundWaitUntil(ingestDeletedDiscordMessage(options, message, state));
    },
    onChannelObserved: async (channel) => {
      if (!isAllowedDiscordGuild(channel.guildId, options)) return;
      backgroundWaitUntil(ingestObservedDiscordChannel(options, channel, state));
    },
    publicKey: options.publicKey,
    mentionRoleIds: options.mentionRoleIds,
    userName,
    logger,
    // Discord delta (patched adapter): gate mentions BEFORE the adapter
    // creates a public thread — a mention rejected by the fail-closed guild
    // allowlist must not mutate the server (no orphan threads in
    // non-allowlisted guilds). The full message gate (isAllowedDiscordMessage)
    // still runs in the handlers below.
    shouldHandleMention: ({ guildId }) =>
      isAllowedDiscordGuild(guildId, options),
    // Discord delta (patched adapter): the gateway drops bot-authored
    // messages by default; forward only allowlisted trigger bots in
    // allowlisted guilds. The allowlist entry must be the id the message is
    // authored as (bot user id, or the webhook id for webhook integrations);
    // isAllowedDiscordMessage applies the broader application_id/webhook_id
    // matching once the full payload is available.
    shouldForwardBotMessage: ({ authorId, guildId }) =>
      isAllowedDiscordGuild(guildId, options) &&
      resolveTriggerBotAllowlist(options).includes(authorId),
    // Discord delta (patched adapter): the Gateway never redelivers, so a
    // message dropped on a thread-lock conflict is otherwise lost with zero
    // signal — surface it with a 🔁 reaction so the user knows to resend.
    onMessageDropped: (info) => {
      logger.warn("discordbot_message_dropped_lock_conflict", {
        channel_id: info.channelId,
        guild_id: info.guildId,
        message_id: info.messageId,
        thread_id: info.threadId,
      });
      void reactToDiscordMessage(
        options,
        {
          emoji: "🔁",
          messageId: info.messageId,
          threadKey: `discord:${info.guildId}:${info.channelId}${
            info.threadId ? `:${info.threadId}` : ""
          }`,
        },
        logger,
      );
    },
    // Discord delta (patched adapter): track gateway liveness so /health goes
    // 503 once the connection has been down for >60s (see gateway.ts).
    onGatewayStatusChange: (connected) => setGatewayConnected(connected),
  });
  const chat = new Chat<{ discord: typeof discord }, DiscordbotThreadState>({
    userName,
    adapters: { discord },
    state,
    // No SDK-level streaming placeholder: instant feedback is the 👀 reaction
    // the narrator puts on the triggering message, and the final answer must
    // land as a NEW message at the bottom of the timeline — with null, the
    // SDK's post+edit fallback only creates the answer message once the first
    // visible answer text arrives.
    fallbackStreamingPlaceholderText: null,
    // Serialize handlers per thread via the SDK's per-thread lock. The deprecated
    // `onLockConflict: 'force'` force-released the lock so two handlers ran concurrently on one
    // thread — two near-simultaneous mentions could both pass the `activeExecution` check and
    // double-execute. `'drop'` keeps the lock: a second message that lands while a handler holds the
    // thread lock is dropped rather than run in parallel. Same code path as before for the
    // no-contention case, so single-message streaming is unchanged.
    concurrency: {
      strategy: "queue",
      maxQueueSize: 1_000,
      onQueueFull: "drop-newest",
      queueEntryTtlMs: 5 * 60 * 1_000,
    },
    // Queue entries must never cross logical Discord conversations. The
    // adapter's channel scope would otherwise drain a message from one thread
    // through another thread's handler.
    lockScope: "thread",
    logger,
  });

  // Discord delta (no slackbotv2 analog): per-guild in-flight execution cap;
  // the pod is a singleton by design, so an in-memory counter is authoritative.
  const executionLimiter = new GuildExecutionLimiter(
    options.maxConcurrentExecutionsPerGuild ??
      DEFAULT_MAX_CONCURRENT_EXECUTIONS_PER_GUILD,
  );

  if (options.applicationIngestionUrl && options.applicationIngestionToken) {
    scheduleApplicationIngestionRecovery(state, options);
  }

  chat.onNewMention(async (thread, message, context) => {
    if (!isAllowedDiscordMessage(message, options, logger)) return;
    await thread.subscribe();
    await syncThreadMessageToSession(thread, message, {
      executionLimiter,
      mode: "execute",
      options,
      precedingMessages: allowedQueuedMessages(context, options, logger),
      state,
    });
  });

  chat.onSubscribedMessage(async (thread, message, context) => {
    if (!isAllowedDiscordMessage(message, options, logger)) return;
    await syncThreadMessageToSession(thread, message, {
      executionLimiter,
      mode: message.isMention === true ? "execute" : "append",
      options,
      precedingMessages: allowedQueuedMessages(context, options, logger),
      state,
    });
  });

  const app = new Hono();
  app.get("/health", (c) => {
    const gatewayActive = options.isGatewayActive
      ? options.isGatewayActive()
      : true;
    return c.json(
      { ok: gatewayActive, service: "discordbot", gateway: gatewayActive },
      gatewayActive ? 200 : 503,
    );
  });

  if (options.recoverRenderObligationsOnStart !== false) {
    scheduleRenderObligationRecovery(chat, state, options);
  }

  return { app, chat, adapter: discord };
}

function allowedQueuedMessages(
  context: MessageContext | undefined,
  options: DiscordbotOptions,
  logger: Logger,
): ChatMessage[] {
  return (context?.skipped ?? []).filter((message) =>
    isAllowedDiscordMessage(message, options, logger),
  );
}

function createDefaultState(
  options: DiscordbotOptions,
  logger: Logger,
): StateAdapter {
  const stateLogger = logger.child("postgres-state");
  // Own the pool so we can attach an error handler. pg.Pool emits 'error' for
  // idle clients whose connection drops (Postgres restart, or a transient blip
  // while the pod's network is still being programmed at startup). With no
  // listener, node-postgres rethrows it as an uncaught exception and the process
  // crashes/spews. Logging and swallowing lets the pool reconnect on the next query.
  const pool = new pg.Pool({ connectionString: options.postgresUrl });
  pool.on("error", (error) => {
    stateLogger.warn("postgres pool error", { error: errorMessage(error) });
  });
  return createPostgresState({
    client: pool,
    keyPrefix: options.stateKeyPrefix ?? "centaur-discordbot",
    logger: stateLogger,
  });
}

/**
 * Blocks until the state backend accepts a connection, retrying with exponential
 * backoff. The first DB connection fires within milliseconds of process start and
 * can lose a race with the pod's network programming (a one-off ECONNREFUSED).
 * Retrying instead of throwing absorbs that race; the first successful connect
 * also flips the adapter's `connected` flag, so the message path comes alive too.
 */
async function ensureStateConnected(
  state: StateAdapter,
  options: DiscordbotOptions,
): Promise<void> {
  for (let attempt = 0; ; attempt++) {
    try {
      await state.connect();
      if (attempt > 0) {
        traceLog(options, "discordbot_postgres_connected", undefined, {
          attempts: attempt + 1,
        });
      }
      return;
    } catch (error) {
      const delayMs = Math.min(
        POSTGRES_CONNECT_INITIAL_DELAY_MS * 2 ** attempt,
        POSTGRES_CONNECT_MAX_DELAY_MS,
      );
      traceLog(options, "discordbot_postgres_connect_retry", undefined, {
        attempt: attempt + 1,
        delay_ms: delayMs,
        error: errorMessage(error),
      });
      await sleep(delayMs);
    }
  }
}

/**
 * Persists a Discord thread update into the session API. In execute mode the create/append/execute
 * handoff completes before the handler returns; SSE rendering continues in background.
 */
async function syncThreadMessageToSession(
  thread: Thread<DiscordbotThreadState>,
  message: ChatMessage,
  input: {
    executionLimiter: GuildExecutionLimiter;
    mode: DiscordbotMessageMode;
    options: DiscordbotOptions;
    precedingMessages?: ChatMessage[];
    state: StateAdapter;
  },
): Promise<void> {
  const traceStartedAtMs = nowMs();
  const logger = input.options.logger ?? noopLogger;
  const state = (await thread.state) ?? {};
  const messageIds = new Set(state.forwardedMessageIds ?? []);
  const executedMessageIds = new Set(state.executedMessageIds ?? []);
  // Discord delta: `state.activeExecution !== true` upstream — a stale flag
  // (crash before the render finally cleared it) must not wedge the thread.
  let shouldStartExecution =
    input.mode === "execute" &&
    !hasLiveActiveExecution(
      state,
      input.options.activeExecutionTtlMs ?? ACTIVE_EXECUTION_TTL_MS,
    ) &&
    !executedMessageIds.has(message.id);
  // Discord delta (no slackbotv2 analog): per-guild in-flight execution cap.
  // On exceed the message is demoted to append-only context and gets a 🚦.
  let releaseExecutionSlot: (() => void) | null = null;
  const guildId = parseDiscordThreadKey(thread.id).guildId;
  if (shouldStartExecution && guildId) {
    releaseExecutionSlot = input.executionLimiter.acquire(guildId);
    if (!releaseExecutionSlot) {
      shouldStartExecution = false;
      traceLog(input.options, "discordbot_forward_guild_execution_capped", {
        includeContext: false,
        messageId: message.id,
        mode: input.mode,
        openStream: false,
        startedAtMs: traceStartedAtMs,
        threadId: thread.id,
      });
      await reactToDiscordMessage(
        input.options,
        { emoji: "🚦", messageId: message.id, threadKey: thread.id },
        logger,
      );
    }
  }
  const shouldIncludeContext =
    shouldStartExecution && state.historyForwarded !== true;
  const isDuplicateIncrementalMessage =
    messageIds.has(message.id) &&
    !(input.precedingMessages ?? []).some(
      (item) => !messageIds.has(item.id),
    ) &&
    !shouldStartExecution &&
    !shouldIncludeContext;
  const trace: DiscordbotTrace = {
    includeContext: shouldIncludeContext,
    messageId: message.id,
    mode: input.mode,
    openStream: shouldStartExecution,
    startedAtMs: traceStartedAtMs,
    threadId: thread.id,
  };
  if (isDuplicateIncrementalMessage) {
    traceLog(input.options, "discordbot_forward_duplicate_skipped", trace);
    return;
  }
  traceLog(input.options, "discordbot_forward_started", trace, {
    active_execution: state.activeExecution === true,
    history_forwarded: state.historyForwarded === true,
  });

  const serializeStartedAtMs = nowMs();
  const serializedMessage = await serializeMessage(message);
  const serializedPrecedingMessages = (
    await Promise.all((input.precedingMessages ?? []).map(serializeMessage))
  ).filter((item) => !isContentlessApiMessage(item));
  traceLog(input.options, "discordbot_forward_message_serialized", trace, {
    attachment_count:
      serializedMessage.attachments.length +
      serializedPrecedingMessages.reduce(
        (count, item) => count + item.attachments.length,
        0,
      ),
    queued_message_count: serializedPrecedingMessages.length,
    phase_ms: elapsedMs(serializeStartedAtMs),
  });

  // Discord delta (no slackbotv2 analog): a sticker-only/forwarded/poll/system
  // mention serializes to empty text with no attachments; executing it would
  // fabricate a synthetic "continue" turn. React ❓ and skip instead.
  if (shouldStartExecution && isContentlessApiMessage(serializedMessage)) {
    releaseExecutionSlot?.();
    traceLog(input.options, "discordbot_forward_contentless_skipped", trace);
    await reactToDiscordMessage(
      input.options,
      { emoji: "❓", messageId: message.id, threadKey: thread.id },
      logger,
    );
    return;
  }

  let context: DiscordbotApiMessage[] | undefined;

  const isInlineReply = Boolean(
    parseDiscordThreadKey(thread.id).replyToMessageId,
  );
  if (shouldIncludeContext && !state.historyForwarded && !isInlineReply) {
    const contextStartedAtMs = nowMs();
    try {
      context = await collectInitialContext(thread, message);
    } catch (error) {
      if (!isDiscordPermissionError(error)) throw error;
      // Discord delta (no slackbotv2 analog): a 403 here (missing Read Message
      // History / Missing Access) previously propagated with total user
      // silence. Best-effort tell the user, settle ❌, and stop cleanly.
      releaseExecutionSlot?.();
      traceLog(
        input.options,
        "discordbot_forward_context_permission_denied",
        trace,
        { error: errorMessage(error) },
      );
      await thread
        .adapter.postMessage(
          discordTurnDeliveryKey(thread.id, message.id),
          {
            raw: "I can't read this channel's history — I'm missing permissions (Read Message History).",
          },
        )
        .catch(() => undefined);
      await reactToDiscordMessage(
        input.options,
        { emoji: "❌", messageId: message.id, threadKey: thread.id },
        logger,
      );
      return;
    }
    // Discord delta: a thread created from a message keeps that starter message
    // in the parent channel, so thread history alone misses it (Slack's
    // conversations.replies includes the parent). Prefer the fetched starter
    // over any thread-starter stub already in the history.
    const starter = await fetchThreadStarterMessage(
      input.options,
      thread.id,
      input.options.logger ?? noopLogger,
    );
    if (starter) {
      context = [starter, ...context.filter((item) => item.id !== starter.id)];
    }
    traceLog(input.options, "discordbot_forward_context_collected", trace, {
      message_count: context.length,
      phase_ms: elapsedMs(contextStartedAtMs),
      starter_included: starter !== null,
    });
  } else {
    if (shouldIncludeContext && isInlineReply) {
      context = [serializedMessage];
    }
    traceLog(input.options, "discordbot_forward_context_skipped", trace, {
      message_count: 1,
    });
  }

  let lastEventId = state.lastEventId ?? 0;
  const renderLease: { release: (() => Promise<void>) | null } = {
    release: null,
  };
  const candidateMessages = context ?? [
    ...serializedPrecedingMessages,
    serializedMessage,
  ];
  const messagesToAppend = candidateMessages.filter(
    (item) => !messageIds.has(item.id),
  );
  // Names the session principal in iron-control; resolved (and cached) here so
  // it rides every create. Cosmetic, so a failed lookup just yields undefined.
  const conversationName = await resolveDiscordConversationName(
    input.options,
    thread.id,
    logger,
  );
  const visibleChannelIds = shouldStartExecution
    ? await resolveVisibleChannelScope(
        input.options,
        serializedMessage,
        thread.id,
        logger,
      )
    : undefined;

  const forwardInput: ForwardSessionInput = {
    actorUserId: shouldStartExecution
      ? serializedMessage.author.userId
      : undefined,
    afterEventId: lastEventId,
    conversationName,
    executeMessage: shouldStartExecution ? serializedMessage : undefined,
    messages: messagesToAppend,
    onEventId: (eventId) => {
      lastEventId = Math.max(lastEventId, eventId);
    },
    openStream: false,
    threadId: thread.id,
    trace,
    visibleChannelIds,
  };

  const commitMessagesAppended = async (): Promise<void> => {
    const latest = (await thread.state) ?? {};
    const latestMessageIds = new Set(latest.forwardedMessageIds ?? []);
    for (const item of messagesToAppend) latestMessageIds.add(item.id);
    // Discord delta: write ONLY the fields this commit owns — setState merges
    // via get-then-set, so echoing fields read earlier (activeExecution,
    // renderObligation) here can resurrect values the background render's
    // finally just cleared. lastEventId takes the max against the freshly-read
    // value so a concurrent stream's higher watermark is never regressed.
    await thread.setState({
      forwardedMessageIds: Array.from(latestMessageIds).slice(-1000),
      historyForwarded: latest.historyForwarded || shouldIncludeContext,
      lastEventId: Math.max(latest.lastEventId ?? 0, lastEventId),
    });
    traceLog(input.options, "discordbot_forward_messages_committed", trace, {
      appended_message_count: messagesToAppend.length,
      forwarded_message_count: Math.min(latestMessageIds.size, 1000),
    });
  };

  const commitExecutionStarted = async (
    execution: DiscordbotExecuteSessionResponse,
  ): Promise<void> => {
    const latest = (await thread.state) ?? {};
    const latestExecutedMessageIds = new Set(latest.executedMessageIds ?? []);
    latestExecutedMessageIds.add(serializedMessage.id);
    // Take the render lease before the obligation becomes visible so a
    // concurrent recovery sweep never claims it while this process is about
    // to render it live (upstream slackbotv2 #522).
    try {
      renderLease.release = await acquireRenderLease(input.state, thread.id);
    } catch (error) {
      traceLog(input.options, "discordbot_render_lease_acquire_failed", trace, {
        error: errorMessage(error),
      });
    }
    await thread.setState({
      activeExecution: true,
      // Discord delta: refresh the staleness timestamp where the flag is
      // legitimately re-confirmed (see ACTIVE_EXECUTION_TTL_MS).
      activeExecutionStartedAt: Date.now(),
      executedMessageIds: Array.from(latestExecutedMessageIds).slice(-1000),
      lastEventId: Math.max(latest.lastEventId ?? 0, lastEventId),
      renderObligation: {
        afterEventId: lastEventId,
        executionId: execution.execution_id,
        message: serializedMessage,
      },
    });
    await indexRenderObligation(input.state, {
      options: input.options,
      threadId: thread.id,
      trace,
    });
    traceLog(input.options, "discordbot_forward_execution_committed", trace, {
      execution_id: execution.execution_id,
      executed_message_count: Math.min(latestExecutedMessageIds.size, 1000),
    });
  };

  if (!shouldStartExecution) {
    if (messagesToAppend.length > 0) {
      await forwardToSessionApi(input.options, forwardInput, {
        onMessagesAppended: commitMessagesAppended,
      });
    }
    traceLog(input.options, "discordbot_forward_complete", trace);
    return;
  }

  try {
    await thread.setState({
      activeExecution: true,
      // Discord delta: staleness timestamp, cleared together with the flag.
      activeExecutionStartedAt: Date.now(),
    });
    traceLog(
      input.options,
      "discordbot_forward_active_execution_marked",
      trace,
    );
    // Create + append the session message only (fast). The execute call blocks
    // ~9s on cold sandbox spin-up (incl. the tool-server sidecar), so it's run
    // inside the render stream below — after the 👀 working reaction lands —
    // instead of before it. executeSession is idempotent
    // (idempotency_key = message id), so a render retry won't re-spawn.
    // Discord delta (no slackbotv2 analog): bounded retry — Slack answers 503
    // so Slack redelivers the webhook on a transient failure here; the Gateway
    // has no redelivery and the message is already dedupe-marked, so without a
    // retry a blip permanently drops the turn.
    await withTransientSessionApiRetry(
      () =>
        forwardToSessionApi(
          input.options,
          { ...forwardInput, executeMessage: undefined, openStream: false },
          { onMessagesAppended: commitMessagesAppended },
        ),
      input.options,
      trace,
    );
    scheduleExecutionRender(
      thread,
      serializedMessage,
      input.options,
      forwardInput,
      () => lastEventId,
      shouldIncludeContext,
      renderLease,
      trace,
      commitExecutionStarted,
      releaseExecutionSlot ?? undefined,
    );
    traceLog(input.options, "discordbot_forward_complete", trace, {
      last_event_id: lastEventId,
    });
  } catch (error) {
    // The live render is not happening; let the recovery sweep claim the
    // obligation (if one was committed) as soon as it scans.
    await renderLease.release?.();
    // Discord delta: release the per-guild slot — the render that would have
    // released it was never scheduled.
    releaseExecutionSlot?.();
    const latest = (await thread.state) ?? {};
    await thread.setState({
      activeExecution: false,
      activeExecutionStartedAt: null,
      lastEventId: Math.max(latest.lastEventId ?? 0, lastEventId),
    });
    // Discord ingress arrives via the Gateway, so there is no webhook retry to request
    // (slackbotv2 answers 503 to make Slack re-deliver). Surface the failure in-thread instead.
    await renderExecutionStream(
      thread,
      streamError(error),
      serializedMessage,
      input.options,
      false,
      trace,
    );
    traceLog(input.options, "discordbot_forward_complete", trace, {
      latest_active_execution: latest.activeExecution === true,
      last_event_id: lastEventId,
    });
  }
}

function scheduleExecutionRender(
  thread: Thread<DiscordbotThreadState>,
  message: DiscordbotApiMessage,
  options: DiscordbotOptions,
  input: ForwardSessionInput,
  getLastEventId: () => number,
  isInitialExecution: boolean,
  renderLease: { release: (() => Promise<void>) | null },
  trace?: DiscordbotTrace,
  onExecutionStarted?: (
    execution: DiscordbotExecuteSessionResponse,
  ) => Promise<void>,
  onSettled?: () => void,
): void {
  const promise = (async () => {
    try {
      let attempt = 0;
      while (true) {
        const result = await renderExecutionAttempt(
          thread,
          message,
          options,
          input,
          getLastEventId,
          isInitialExecution,
          trace,
          onExecutionStarted,
          onSettled,
        );
        if (result === "complete") return;
        // Discord delta (no slackbotv2 analog): cap the retry loop — see
        // RENDER_RETRY_MAX_ATTEMPTS. On exhaustion, clear the active flag and
        // surface a failure (❌) instead of duplicating posts forever; the
        // persisted render obligation still lets the next restart retry.
        if (attempt >= RENDER_RETRY_MAX_ATTEMPTS) {
          traceLog(options, "discordbot_render_retries_exhausted", trace, {
            retry_attempts: attempt,
          });
          const latest = (await thread.state) ?? {};
          await thread.setState({
            activeExecution: false,
            activeExecutionStartedAt: null,
            lastEventId: Math.max(latest.lastEventId ?? 0, getLastEventId()),
          });
          await renderExecutionStream(
            thread,
            streamError(
              new Error(
                "Streaming retries exhausted; giving up on rendering this run.",
              ),
            ),
            message,
            options,
            false,
            trace,
          ).catch(() => undefined);
          return;
        }
        const delayMs = renderRetryDelayMs(attempt, options);
        attempt += 1;
        traceLog(options, "discordbot_render_retry_scheduled", trace, {
          retry_delay_ms: delayMs,
          retry_attempt: attempt,
        });
        await sleep(delayMs);
      }
    } finally {
      // The render settled (or gave up): hand the obligation back to the
      // recovery sweep's jurisdiction (upstream slackbotv2 #522).
      await renderLease.release?.();
      // Discord delta: reliably release the per-guild execution slot.
      onSettled?.();
    }
  })();
  backgroundWaitUntil(promise);
}

/**
 * Discord delta (no slackbotv2 analog): treat a persisted `activeExecution`
 * flag as live only while its staleness timestamp is within the TTL. A crash
 * between marking the flag and the render finally clearing it would otherwise
 * block `shouldStartExecution` forever (Gateway ingress has no redelivery).
 * Flags without a timestamp (written by older code, so necessarily from
 * before the last restart) count as stale.
 */
export function hasLiveActiveExecution(
  state: Pick<
    DiscordbotThreadState,
    "activeExecution" | "activeExecutionStartedAt"
  >,
  ttlMs: number,
  nowEpochMs = Date.now(),
): boolean {
  if (state.activeExecution !== true) return false;
  if (typeof state.activeExecutionStartedAt !== "number") return false;
  return nowEpochMs - state.activeExecutionStartedAt <= ttlMs;
}

/**
 * Discord delta (no slackbotv2 analog): bounded retry for the synchronous
 * create/append phase. Only retryable-classified failures are retried, with
 * the FORWARD_RETRY_DELAYS_MS backoff (3 attempts total).
 */
async function withTransientSessionApiRetry<T>(
  operation: () => Promise<T>,
  options: DiscordbotOptions,
  trace?: DiscordbotTrace,
): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    try {
      return await operation();
    } catch (error) {
      const delayMs = FORWARD_RETRY_DELAYS_MS[attempt];
      if (delayMs === undefined || !isRetryableSessionApiError(error)) {
        throw error;
      }
      traceLog(options, "discordbot_forward_transient_retry", trace, {
        attempt: attempt + 1,
        delay_ms: delayMs,
        error: errorMessage(error),
      });
      await sleep(delayMs);
    }
  }
}

async function renderExecutionAttempt(
  thread: Thread<DiscordbotThreadState>,
  message: DiscordbotApiMessage,
  options: DiscordbotOptions,
  input: ForwardSessionInput,
  getLastEventId: () => number,
  isInitialExecution: boolean,
  trace?: DiscordbotTrace,
  onExecutionStarted?: (
    execution: DiscordbotExecuteSessionResponse,
  ) => Promise<void>,
  onExecutionSettled?: () => void,
): Promise<"complete" | "retry"> {
  let rendered = false;
  let retry = false;
  try {
    await renderExecutionStream(
      thread,
      streamSessionAfterHandoff(options, input, onExecutionStarted),
      message,
      options,
      isInitialExecution,
      trace,
    );
    rendered = true;
    traceLog(options, "discordbot_render_complete", trace);
    return "complete";
  } catch (error) {
    if (isRetryableSessionApiError(error)) {
      retry = true;
      traceLog(options, "discordbot_render_deferred", trace, {
        error: errorMessage(error),
        last_event_id: getLastEventId(),
      });
      return "retry";
    }
    traceLog(options, "discordbot_render_failed", trace, {
      error: errorMessage(error),
    });
    throw error;
  } finally {
    const latest = (await thread.state) ?? {};
    await thread.setState({
      activeExecution: retry,
      // Discord delta: refresh the staleness timestamp while a retry keeps
      // the flag alive; clear both together otherwise.
      activeExecutionStartedAt: retry ? Date.now() : null,
      lastEventId: Math.max(latest.lastEventId ?? 0, getLastEventId()),
      ...(rendered ? { renderObligation: null } : {}),
    });
    // The public reaction lifecycle and durable thread state now agree that
    // this attempt is terminal. Release guild capacity before best-effort
    // render-lease cleanup, which must not delay the next conversation.
    if (!retry) onExecutionSettled?.();
    traceLog(options, "discordbot_render_finalized", trace, {
      obligation_cleared: rendered,
      retry_scheduled: retry,
      last_event_id: getLastEventId(),
    });
  }
}

function scheduleRenderObligationRecovery(
  chat: Chat<Record<string, Adapter>, DiscordbotThreadState>,
  state: StateAdapter,
  options: DiscordbotOptions,
): void {
  backgroundWaitUntil(recoverRenderObligationsWithRetry(chat, state, options));
}

function scheduleApplicationIngestionRecovery(
  state: StateAdapter,
  options: DiscordbotOptions,
): void {
  backgroundWaitUntil(
    (async () => {
      await ensureStateConnected(state, options);
      const recoverOutbox = async () => {
        try {
          const pending = await recoverApplicationIngestion(options, state);
          if (pending > 0) {
            traceLog(options, "discordbot_application_ingestion_pending", undefined, {
              pending_count: pending,
            });
          }
        } catch (error) {
          traceLog(options, "discordbot_application_ingestion_recovery_failed", undefined, {
            error: errorMessage(error),
          });
        }
      };
      const reconcileArchive = singleFlight(async () => {
        try {
          const observed = await reconcileDiscordArchive(options, state);
          if (observed > 0) {
            traceLog(options, "discordbot_application_archive_reconciled", undefined, {
              observed_count: observed,
            });
          }
        } catch (error) {
          traceLog(options, "discordbot_application_archive_reconciliation_failed", undefined, {
            error: errorMessage(error),
          });
        }
      });
      const archiveReconciliationEnabled =
        options.applicationArchiveReconciliationEnabled !== false;
      await Promise.all([
        recoverOutbox(),
        ...(archiveReconciliationEnabled ? [reconcileArchive()] : []),
      ]);
      const outboxTimer = setInterval(
        () => backgroundWaitUntil(recoverOutbox()),
        5_000,
      );
      outboxTimer.unref();
      if (archiveReconciliationEnabled) {
        const archiveTimer = setInterval(
          () => backgroundWaitUntil(reconcileArchive()),
          Math.max(
            1_000,
            options.applicationArchiveReconciliationIntervalMs ?? 60_000,
          ),
        );
        archiveTimer.unref();
      }
    })(),
  );
}

async function recoverRenderObligationsWithRetry(
  chat: Chat<Record<string, Adapter>, DiscordbotThreadState>,
  state: StateAdapter,
  options: DiscordbotOptions,
): Promise<void> {
  // Wait for Postgres before scanning for obligations. This is also what warms the
  // shared pool at startup, so transient connect failures don't wedge the bot.
  await ensureStateConnected(state, options);
  let attempt = 0;
  while (true) {
    try {
      const deferredCount = await recoverRenderObligations(
        chat,
        state,
        options,
      );
      if (deferredCount === 0) return;
      const delayMs = renderRetryDelayMs(attempt, options);
      attempt += 1;
      traceLog(
        options,
        "discordbot_render_recovery_retry_scheduled",
        undefined,
        {
          deferred_count: deferredCount,
          retry_delay_ms: delayMs,
          retry_attempt: attempt,
        },
      );
      await sleep(delayMs);
    } catch (error) {
      traceLog(options, "discordbot_render_recovery_failed", undefined, {
        error: errorMessage(error),
      });
      return;
    }
  }
}

async function recoverRenderObligations(
  chat: Chat<Record<string, Adapter>, DiscordbotThreadState>,
  state: StateAdapter,
  options: DiscordbotOptions,
): Promise<number> {
  const startedAtMs = nowMs();
  await chat.initialize();
  const indexedThreadIds = await state.getList<string>(
    RENDER_OBLIGATION_INDEX_KEY,
  );
  const threadIds = Array.from(new Set(indexedThreadIds));
  let deferredCount = 0;
  traceLog(options, "discordbot_render_recovery_scan", undefined, {
    obligation_count: threadIds.length,
    phase_ms: elapsedMs(startedAtMs),
  });

  for (const threadId of threadIds) {
    try {
      const thread = chat.thread(threadId);
      const threadState = await thread.state;
      const obligation = threadState?.renderObligation;
      if (!obligation) continue;

      const leaseToken = randomUUID();
      const leaseAcquired = await state.setIfNotExists(
        renderRecoveryLeaseKey(threadId),
        leaseToken,
        RENDER_RECOVERY_LEASE_TTL_MS,
      );
      if (!leaseAcquired) {
        traceLog(
          options,
          "discordbot_render_recovery_lease_skipped",
          undefined,
          {
            thread_id: threadId,
          },
        );
        continue;
      }

      try {
        // Discord delta (no slackbotv2 analog): the obligation above was read
        // BEFORE the lease; another worker may have completed or replaced it
        // in between. Re-read under the lease and recover the current value.
        const leasedObligation = (await thread.state)?.renderObligation;
        if (!leasedObligation) {
          traceLog(
            options,
            "discordbot_render_recovery_obligation_gone",
            undefined,
            { thread_id: threadId },
          );
          continue;
        }
        if (
          await recoverRenderObligation(
            chat,
            state,
            options,
            threadId,
            leasedObligation,
          )
        ) {
          deferredCount += 1;
        }
      } finally {
        const activeLeaseToken = await state.get<string>(
          renderRecoveryLeaseKey(threadId),
        );
        if (activeLeaseToken === leaseToken) {
          await state.delete(renderRecoveryLeaseKey(threadId));
        }
      }
    } catch (error) {
      // One thread's corrupt state or failed render must not abort the scan:
      // log it, count it as deferred so a later pass retries it, and keep
      // recovering the remaining threads.
      deferredCount += 1;
      traceLog(options, "discordbot_render_recovery_thread_failed", undefined, {
        error: errorMessage(error),
        thread_id: threadId,
      });
    }
  }
  return deferredCount;
}

async function recoverRenderObligation(
  chat: Chat<Record<string, Adapter>, DiscordbotThreadState>,
  state: StateAdapter,
  options: DiscordbotOptions,
  threadId: string,
  obligation: DiscordbotRenderObligation,
): Promise<boolean> {
  const trace: DiscordbotTrace = {
    includeContext: false,
    messageId: obligation.message.id,
    mode: "execute",
    openStream: true,
    startedAtMs: nowMs(),
    threadId,
  };
  const thread = chat.thread(threadId);
  const threadState = (await thread.state) ?? {};
  let lastEventId = Math.max(
    threadState.lastEventId ?? 0,
    obligation.afterEventId,
  );
  const input: ForwardSessionInput = {
    afterEventId: lastEventId,
    executionId: obligation.executionId,
    messages: [],
    onEventId: (eventId) => {
      lastEventId = Math.max(lastEventId, eventId);
    },
    openStream: false,
    threadId,
    trace,
  };

  let openedStream: AsyncIterable<DiscordbotRendererSource>;
  try {
    openedStream = await openSessionEventStream(options, input);
  } catch (error) {
    const retryable = isRetryableSessionApiError(error);
    traceLog(options, "discordbot_render_recovery_deferred", trace, {
      error: errorMessage(error),
      last_event_id: lastEventId,
      retryable,
    });
    if (retryable) return true;
    await renderRecoveredExecutionStream(
      thread,
      streamError(error),
      obligation.message,
      options,
      trace,
    );
    await thread.setState({
      activeExecution: false,
      activeExecutionStartedAt: null,
      lastEventId,
      renderObligation: null,
    });
    return false;
  }

  let rendered = false;
  try {
    await thread.setState({
      activeExecution: true,
      // Discord delta: staleness timestamp, cleared together with the flag.
      activeExecutionStartedAt: Date.now(),
      lastEventId,
    });
    await renderRecoveredExecutionStream(
      thread,
      streamOpenedSession(input, openedStream),
      obligation.message,
      options,
      trace,
    );
    rendered = true;
    traceLog(options, "discordbot_render_recovery_complete", trace);
  } catch (error) {
    traceLog(options, "discordbot_render_recovery_render_failed", trace, {
      error: errorMessage(error),
    });
    throw error;
  } finally {
    const latest = (await thread.state) ?? {};
    await thread.setState({
      activeExecution: false,
      activeExecutionStartedAt: null,
      lastEventId: Math.max(latest.lastEventId ?? 0, lastEventId),
      ...(rendered ? { renderObligation: null } : {}),
    });
    traceLog(options, "discordbot_render_recovery_finalized", trace, {
      obligation_cleared: rendered,
      last_event_id: lastEventId,
    });
  }
  return false;
}

async function indexRenderObligation(
  state: StateAdapter,
  input: {
    options: DiscordbotOptions;
    threadId: string;
    trace?: DiscordbotTrace;
  },
): Promise<void> {
  await state.appendToList(RENDER_OBLIGATION_INDEX_KEY, input.threadId, {
    maxLength: RENDER_OBLIGATION_INDEX_MAX_LENGTH,
    ttlMs: RENDER_INDEX_TTL_MS,
  });
  traceLog(input.options, "discordbot_render_obligation_indexed", input.trace);
}

async function* streamOpenedSession(
  input: Pick<ForwardSessionInput, "threadId" | "trace">,
  stream: AsyncIterable<DiscordbotRendererSource>,
): AsyncIterable<DiscordbotRendererSource> {
  // Deliberate delta from slackbotv2 (which removed its synthetic starting
  // task): the synthetic item primes the mapper's task state so answer deltas
  // stream immediately instead of waiting out the pre-stream grace period.
  yield startingStreamNotification(input.threadId);
  for await (const event of stream) yield event;
}

function renderRecoveryLeaseKey(threadId: string): string {
  return `discordbot:render:lease:${threadId}`;
}

/**
 * Holds the per-thread render lease for the duration of a live render so the
 * recovery sweep cannot claim the just-indexed obligation and post a
 * duplicate answer (it lease-skips instead). The TTL keeps this crash-safe:
 * if the pod dies mid-render the lease expires and recovery takes over. The
 * lease is refreshed while the render runs because agent turns routinely
 * outlive a single TTL window. (Ported from slackbotv2 #522.)
 */
async function acquireRenderLease(
  state: StateAdapter,
  threadId: string,
): Promise<() => Promise<void>> {
  const key = renderRecoveryLeaseKey(threadId);
  const token = randomUUID();
  await state.set(key, token, RENDER_RECOVERY_LEASE_TTL_MS);
  const refresh = setInterval(() => {
    void state
      .get<string>(key)
      .then((current) =>
        current === token
          ? state.set(key, token, RENDER_RECOVERY_LEASE_TTL_MS)
          : undefined,
      )
      .catch(() => undefined);
  }, RENDER_LEASE_REFRESH_INTERVAL_MS);
  return async () => {
    clearInterval(refresh);
    try {
      const current = await state.get<string>(key);
      if (current === token) await state.delete(key);
    } catch {
      // Best effort: TTL expiry is the backstop.
    }
  };
}

async function renderExecutionStream(
  thread: Thread,
  stream: AsyncIterable<DiscordbotRendererSource>,
  message: DiscordbotApiMessage,
  options: DiscordbotOptions,
  isInitialExecution: boolean,
  trace?: DiscordbotTrace,
): Promise<void> {
  const logger = options.logger ?? noopLogger;
  const responseFooter = (): string | undefined =>
    buildDiscordResponseFooter({
      elapsedMs:
        trace && options.responseLatencyEnabled === true
          ? Math.max(0, nowMs() - trace.startedAtMs)
          : undefined,
      isFirstAssistantMessage: isInitialExecution,
      options,
    });
  if (
    isInitialExecution &&
    options.nameThreads !== false &&
    isThreadCreatedForMessage(thread.id, message.id)
  ) {
    await renameThreadFromMessage(
      options,
      thread.id,
      deriveThreadName(message.text, options.userName),
      logger,
    );
    traceLog(options, "discordbot_thread_named", trace);
  }

  if (isPlainTextOnlyRequest(message.text)) {
    // Discord delta (no slackbotv2 analog): plain-text-only runs previously
    // bypassed the narrator entirely — no 👀, no ✅/❌. Reuse the narrator for
    // the reaction lifecycle only (update() is never called, so no reasoning
    // blurbs are posted; the run still produces a single plain-text message).
    const narrator = DiscordNarrator.start(thread, message, options, {
      logger,
    });
    try {
      const sawError = await renderPlainTextExecutionStream(
        thread,
        stream,
        options,
        discordTurnDeliveryKey(thread.id, message.id),
        trace,
        responseFooter,
      );
      await narrator.finish(sawError ? "failed" : "done");
    } catch (error) {
      await narrator.finish(
        isRetryableSessionApiError(error) ? "retrying" : "failed",
      );
      throw error;
    }
    return;
  }

  // Append-only narration: an instant 👀 reaction on the triggering message,
  // then the agent's reasoning blurbs posted as their own subtext (-#)
  // messages as each thought completes, then the completed answer posted as
  // a separate message. On settle the 👀 flips to ✅/❌. No bot message is
  // ever edited or deleted, so messages keep their place in the timeline even
  // when users chime in mid-run.
  const narrator = DiscordNarrator.start(thread, message, options, { logger });
  const stopTyping = startTypingKeepalive(thread, logger);
  try {
    await renderSplitExecutionStreams(
      thread,
      stream,
      options,
      narrator,
      discordTurnDeliveryKey(thread.id, message.id),
      responseFooter,
    );
    await narrator.finish("done");
  } catch (error) {
    await narrator.finish(
      isRetryableSessionApiError(error) ? "retrying" : "failed",
    );
    throw error;
  } finally {
    stopTyping();
  }
}

/**
 * Consumes the renderer's chunk stream, routing task updates to the narrator
 * (reasoning blurbs) while buffering answer deltas. The completed answer is
 * posted only after the source stream settles successfully; the synthetic
 * startingStreamNotification item only unblocks deltas and never creates an
 * empty message.
 */
async function renderSplitExecutionStreams(
  thread: Thread,
  stream: AsyncIterable<DiscordbotRendererSource>,
  options: DiscordbotOptions,
  narrator: DiscordNarrator,
  deliveryThreadId: string,
  responseFooter?: () => string | undefined,
): Promise<void> {
  let answerText = "";
  for await (const chunk of harnessToChatSdkStream(
    stream,
    rendererOptions(options, narrator),
  )) {
    if (chunk.type === "markdown_text") {
      answerText += chunk.text;
      continue;
    }
    narrator.update(chunk);
  }
  await postBufferedAnswerToThread(
    thread,
    appendDiscordResponseFooter(answerText, responseFooter?.()),
    deliveryThreadId,
  );
}

/**
 * Discord delta (replaces the chat SDK's post+edit fallbackStream): posts a
 * completed answer across multiple messages when necessary, each no longer
 * than ANSWER_MESSAGE_MAX_CHARS, splitting at newline/whitespace boundaries
 * (never through a surrogate pair; code fences are closed and re-opened when
 * a split inside one is unavoidable). Past ANSWER_MAX_FULL_MESSAGES the
 * remainder collapses into one final honestly-truncated message
 * ("[truncated N chars ...]", never a silent "..."). Posting failures
 * propagate so the run is not falsely marked successful.
 * Exported for tests only.
 */
export async function postBufferedAnswerToThread(
  thread: Thread,
  answer: string,
  deliveryThreadId = thread.id,
): Promise<void> {
  let pending = answer;
  let postedCount = 0;

  const postNew = async (content: string): Promise<void> => {
    // `raw` skips the SDK's markdown round-trip: re-stringifying could change
    // the text length past Discord's 2000-char cap (re-triggering the
    // adapter's silent truncation) and Discord renders markdown natively.
    const visibleContent = suppressDiscordLinkEmbeds(content);
    await thread.adapter.postMessage(deliveryThreadId, {
      raw: visibleContent,
    });
    postedCount += 1;
  };

  const overflowed = (): boolean => postedCount >= ANSWER_MAX_FULL_MESSAGES;
  while (!overflowed()) {
    const split = takeDiscordMessageChunk(pending, ANSWER_MESSAGE_MAX_CHARS);
    if (!split) break;
    await postNew(split.chunk);
    pending = split.rest;
  }
  if (!pending.trim()) return;
  await postNew(
    overflowed()
      ? truncateDiscordText(
          pending,
          ANSWER_MESSAGE_MAX_CHARS,
          "Discord final answer",
        )
      : pending,
  );
}

async function renderRecoveredExecutionStream(
  thread: Thread,
  stream: AsyncIterable<DiscordbotRendererSource>,
  message: DiscordbotApiMessage,
  options: DiscordbotOptions,
  trace?: DiscordbotTrace,
): Promise<void> {
  // Recovered renders never rename the thread; naming happens on the initial execution.
  // The narration/answer message split (and the plain-text-only branch) comes
  // via renderExecutionStream.
  await renderExecutionStream(thread, stream, message, options, false, trace);
}

async function renderPlainTextExecutionStream(
  thread: Thread,
  stream: AsyncIterable<DiscordbotRendererSource>,
  options: DiscordbotOptions,
  deliveryThreadId: string,
  trace?: DiscordbotTrace,
  responseFooter?: () => string | undefined,
): Promise<boolean> {
  const logger = options.logger ?? noopLogger;
  const fallback = new DiscordRenderFallback();
  const stopTyping = startTypingKeepalive(thread, logger);
  // Discord delta: reported back so the caller can settle the reaction
  // lifecycle as ❌ when the renderer surfaced an in-stream failure.
  let sawError = false;
  try {
    const chatStream = fallback.collectChatSdk(
      harnessToChatSdkStream(
        fallback.collectSource(stream),
        rendererOptions(options),
      ),
    );
    for await (const chunk of chatStream) {
      if (chunk.type === "task_update" && chunk.status === "error") {
        sawError = true;
      }
    }
    const text = suppressDiscordLinkEmbeds(
      truncateDiscordText(
        appendDiscordResponseFooter(
          fallback.text() ||
            "Execution completed, but no final text was captured.",
          responseFooter?.(),
        ),
        DISCORD_FALLBACK_TEXT_MAX_CHARS,
        "Discord final answer",
      ),
    );
    traceLog(options, "discordbot_render_plain_text_final", trace, {
      chars: text.length,
    });
    await thread.adapter.postMessage(deliveryThreadId, { raw: text });
  } finally {
    stopTyping();
  }
  return sawError;
}

export function buildDiscordResponseFooter(input: {
  elapsedMs?: number;
  isFirstAssistantMessage: boolean;
  options: Pick<
    DiscordbotOptions,
    | "responseLatencyEnabled"
    | "responseMetadataHarness"
    | "responseMetadataMode"
    | "responseMetadataModel"
    | "responseMetadataReasoning"
  >;
}): string | undefined {
  const mode = input.options.responseMetadataMode ?? "first";
  if (
    mode === "never" ||
    (mode === "first" && !input.isFirstAssistantMessage)
  ) {
    return undefined;
  }
  const segments: string[] = [];
  const model = input.options.responseMetadataModel?.trim();
  if (model) segments.push(model.toUpperCase());
  const harness = titleCaseDisplay(input.options.responseMetadataHarness);
  if (harness) segments.push(harness);
  const reasoning = titleCaseDisplay(input.options.responseMetadataReasoning);
  if (reasoning) segments.push(reasoning);
  if (
    input.options.responseLatencyEnabled === true &&
    input.elapsedMs !== undefined
  ) {
    segments.push(formatResponseLatency(input.elapsedMs));
  }
  return segments.length > 0 ? `-# ${segments.join(" · ")}` : undefined;
}

function appendDiscordResponseFooter(answer: string, footer?: string): string {
  if (!footer) return answer;
  return `${answer.trimEnd()}\n\n${footer}`;
}

function titleCaseDisplay(value: string | undefined): string | undefined {
  const normalized = value?.trim();
  if (!normalized) return undefined;
  const key = normalized.toLowerCase();
  if (key === "codex") return "Codex";
  if (key === "nanocodex") return "Nanocodex";
  if (key === "claudecode") return "Claude Code";
  return key
    .split(/[\s_-]+/)
    .filter(Boolean)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}

function formatResponseLatency(milliseconds: number): string {
  if (milliseconds < 1_000) return `${Math.round(milliseconds)}ms`;
  return `${(milliseconds / 1_000).toFixed(1)}s`;
}

class DiscordRenderFallback {
  private markdownText = "";
  private terminalText = "";

  async *collectSource(
    stream: AsyncIterable<DiscordbotRendererSource>,
  ): AsyncIterable<DiscordbotRendererSource> {
    for await (const event of stream) {
      this.captureTerminalText(event);
      yield event;
    }
  }

  async *collectChatSdk(
    stream: AsyncIterable<ChatSDKStreamChunk>,
  ): AsyncIterable<ChatSDKStreamChunk> {
    for await (const chunk of stream) {
      if (chunk.type === "markdown_text") this.markdownText += chunk.text;
      yield chunk;
    }
  }

  text(): string {
    return (this.terminalText || this.markdownText).trim();
  }

  private captureTerminalText(event: DiscordbotRendererSource): void {
    if (!event || typeof event !== "object") return;
    const eventKind = String(
      "eventKind" in event
        ? event.eventKind
        : "event" in event
          ? event.event
          : "",
    );
    if (
      eventKind !== "session.execution_completed" &&
      eventKind !== "session.execution_cancelled" &&
      !isTerminalCodexAppServerEvent(event)
    ) {
      return;
    }
    const data =
      "data" in event && event.data && typeof event.data === "object"
        ? event.data
        : event;
    const text = terminalResultText(data);
    if (text) this.terminalText = text;
  }
}

function isTerminalCodexAppServerEvent(event: unknown): boolean {
  if (!event || typeof event !== "object") return false;
  const type = (event as { type?: unknown }).type;
  return type === "result" || type === "turn.done" || type === "turn.completed";
}

function terminalResultText(event: unknown): string {
  if (!event || typeof event !== "object") return "";
  for (const key of ["result", "result_text", "text", "final_text"]) {
    const value = (event as Record<string, unknown>)[key];
    if (typeof value !== "string") continue;
    const resultText = value.trim();
    if (resultText) return resultText;
  }
  return "";
}

function isPlainTextOnlyRequest(text: string): boolean {
  const normalized = text.toLowerCase();
  return (
    /\bplain\s+text\s+only\b/.test(normalized) ||
    /\bno\s+interactive\s+blocks?\b/.test(normalized) ||
    /\bno\s+dashboards?\b/.test(normalized)
  );
}

function truncateDiscordText(
  value: string,
  maxChars: number,
  label: string,
): string {
  if (value.length <= maxChars) return value;
  let omitted = value.length - maxChars;
  while (true) {
    const suffix = `\n[truncated ${omitted} chars from ${label}]`;
    const keep = Math.max(0, maxChars - suffix.length);
    const actualOmitted = value.length - keep;
    if (actualOmitted === omitted)
      // Discord delta: surrogate-safe cut — halving an emoji's surrogate pair
      // makes Discord reject the whole payload with a 400.
      return `${sliceSurrogateSafe(value, keep).trimEnd()}${suffix}`;
    omitted = actualOmitted;
  }
}

async function* streamSessionAfterHandoff(
  options: DiscordbotOptions,
  input: ForwardSessionInput,
  onExecutionStarted?: (
    execution: DiscordbotExecuteSessionResponse,
  ) => Promise<void>,
): AsyncIterable<DiscordbotRendererSource> {
  // The 👀 working reaction is already queued before this generator is
  // consumed, so the user has instant feedback while the cold sandbox (incl.
  // tool-server sidecar) spends ~9s spinning up. Execute runs here, inside the
  // render stream, so a sandbox-spawn failure surfaces in the same render
  // rather than leaving the run looking alive forever (api-rs writes no event
  // if the spawn itself fails). The synthetic starting item primes the
  // mapper's task state so answer deltas stream without the pre-stream grace
  // delay.
  yield startingStreamNotification(input.threadId);
  traceLog(options, "discordbot_stream_heartbeat_emitted", input.trace);

  if (input.executeMessage) {
    try {
      const execution = await executeSessionTurn(options, input);
      if (execution) {
        // Scope the event stream we open below to this execution (upstream
        // #422 sets this where execute returns; for us that's in-stream).
        input.executionId = execution.execution_id;
        await onExecutionStarted?.(execution);
      }
    } catch (error) {
      traceLog(options, "discordbot_forward_failed", input.trace, {
        error: errorMessage(error),
      });
      if (isRetryableSessionApiError(error)) throw error;
      yield sessionStreamError(error);
      return;
    }
  }

  let stream: AsyncIterable<DiscordbotRendererSource>;
  try {
    stream = await openSessionEventStream(options, input);
  } catch (error) {
    traceLog(options, "discordbot_forward_failed", input.trace, {
      error: errorMessage(error),
    });
    if (isRetryableSessionApiError(error)) throw error;
    yield sessionStreamError(error);
    return;
  }

  for await (const event of stream) yield event;
}

async function* streamError(
  error: unknown,
): AsyncIterable<DiscordbotRendererSource> {
  yield sessionStreamError(error);
}

function backgroundWaitUntil(promise: Promise<unknown>): void {
  // Discord ingress runs in a long-lived Gateway process (no per-request waitUntil);
  // background work just needs its rejections swallowed after they are traced.
  void promise.catch(() => undefined);
}

// Mirrors slackbotv2's rendererOptions: forwards the configured mapper and
// hooks onRendererEvent. Slack routes renderer.status (server-side activity
// summaries) to its native assistant status; Discord has no such surface, so
// they post as the narrator's append-only subtext blurbs. Paths without a
// narrator (plain-text runs) drop status events by design.
function rendererOptions(
  options: DiscordbotOptions,
  narrator?: DiscordNarrator,
): CodexAppServerToChatStreamOptions {
  const mapper = options.mapper;
  return {
    ...mapper,
    // Discord streams answer text into its own append-only messages, so
    // there is no card to wait for: stream deltas immediately. Non-zero
    // grace here can strand an answer-only turn's deltas entirely — the
    // grace check is event-driven, and with no tasks and no later events
    // the buffered text only flushes at stream end.
    preStreamGraceMs: 0,
    async onRendererEvent(event: RendererEvent) {
      await mapper?.onRendererEvent?.(event);
      if (event.type === "renderer.status") {
        narrator?.status(event.status);
      }
    },
  };
}

function renderRetryDelayMs(
  attempt: number,
  options: Pick<
    DiscordbotOptions,
    "renderRetryInitialDelayMs" | "renderRetryMaxDelayMs"
  >,
): number {
  const initialDelayMs =
    options.renderRetryInitialDelayMs ?? RENDER_RETRY_INITIAL_DELAY_MS;
  const maxDelayMs =
    options.renderRetryMaxDelayMs ?? RENDER_RETRY_MAX_DELAY_MS;
  return Math.min(
    initialDelayMs * 2 ** attempt,
    maxDelayMs,
  );
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Discord's typing indicator expires after ~10s, so a single call blinks off mid-run. Re-fire on
 * an interval while the stream is open; errors are swallowed (typing is cosmetic) and the interval
 * is always cleared by the returned stop function.
 */
function startTypingKeepalive(thread: Thread, logger: Logger): () => void {
  const adapter = thread.adapter as TypingCapableAdapter;
  if (!adapter.startTyping) return () => undefined;

  const fire = (): void => {
    void adapter.startTyping?.(thread.id).catch((error) => {
      logger.debug("discordbot_typing_error", {
        error: error instanceof Error ? error.message : String(error),
      });
    });
  };
  fire();
  const interval = globalThis.setInterval(fire, TYPING_KEEPALIVE_MS);
  return () => globalThis.clearInterval(interval);
}
