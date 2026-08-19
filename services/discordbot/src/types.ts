import type { RustSessionStreamEvent } from "@centaur/harness-events";
import type { CodexAppServerToChatStreamOptions } from "@centaur/rendering";
import type { Attachment, Chat, Logger, StateAdapter } from "chat";
import type { Hono } from "hono";
import type { ApplicationIngestionOutbox } from "./application-ingestion-outbox";

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonObject | JsonValue[];
export type JsonObject = { [key: string]: JsonValue | undefined };

export type DiscordbotApiAuthor = {
  fullName: string;
  isBot: boolean | "unknown";
  isMe: boolean;
  userId: string;
  userName: string;
};

export type DiscordbotApiAttachment = {
  dataBase64?: string;
  dataBase64Omitted?: string;
  fetchError?: string;
  fetchMetadata?: Record<string, string>;
  height?: number;
  mimeType?: string;
  name?: string;
  size?: number;
  type: Attachment["type"];
  url?: string;
  width?: number;
};

export type DiscordbotApiMessage = {
  attachments: DiscordbotApiAttachment[];
  author: DiscordbotApiAuthor;
  id: string;
  isMention: boolean;
  raw: unknown;
  text: string;
  threadId: string;
  timestamp: string;
};

export type DiscordbotSessionMessageRole =
  | "user"
  | "assistant"
  | "system"
  | "tool";

export type DiscordbotSessionMessage = {
  client_message_id?: string;
  metadata: JsonObject;
  parts: JsonValue[];
  role: DiscordbotSessionMessageRole;
};

export type DiscordbotAppendMessagesRequest = {
  messages: DiscordbotSessionMessage[];
};

export type DiscordbotCreateSessionRequest = {
  chat_destination?: DiscordbotChatDestination;
  harness_type: string;
  metadata: JsonObject;
};

export type DiscordbotChatDestination = {
  platform: "discord";
  guild_id: string;
  channel_id: string;
  thread_id: string | null;
  reply_to_message_id?: string;
};

export type DiscordbotExecuteSessionRequest = {
  idempotency_key?: string;
  idle_timeout_ms?: number;
  input_lines: string[];
  invocation: DiscordbotInvocationContext;
  max_duration_ms?: number;
  metadata: JsonObject;
};

export type DiscordbotInvocationContext = {
  version: 1;
  kind: "discord_member";
  actor: {
    platform: "discord";
    user_id: string;
    guild_id: string;
  };
  conversation: {
    platform: "discord";
    channel_id: string;
    thread_id: string | null;
  };
  source: {
    event_id: string;
    message_id: string;
  };
  authority: {
    mutation: "current_member_request";
    observed_at: string;
    visible_channel_ids: string[];
  };
};

export type DiscordbotExecuteSessionResponse = {
  execution_id: string;
  ok: boolean;
  status: string;
  thread_key: string;
};

export type DiscordbotFetch = (
  input: RequestInfo | URL,
  init?: RequestInit,
) => Promise<Response>;

export type DiscordbotOptions = {
  /**
   * Discord delta: TTL after which a persisted `activeExecution` flag is
   * treated as stale (a crash between marking and clearing would otherwise
   * wedge the thread forever — Gateway ingress has no redelivery to kick it).
   */
  activeExecutionTtlMs?: number;
  apiKey?: string;
  apiUrl: string;
  /** Private application Discord ingestion endpoint; requires the token too. */
  applicationIngestionUrl?: string;
  applicationIngestionToken?: string;
  applicationIngestionOutbox?: ApplicationIngestionOutbox;
  applicationIngestionDeliveryTimeoutMs?: number;
  applicationIngestionRecoveryBatchSize?: number;
  applicationIngestionRecoveryConcurrency?: number;
  applicationIngestionRecoveryLeaseMs?: number;
  /** Periodically reconcile Discord REST history into application ingestion. */
  applicationArchiveReconciliationEnabled?: boolean;
  applicationArchiveReconciliationIntervalMs?: number;
  applicationArchiveReconciliationConcurrency?: number;
  applicationId: string;
  botToken: string;
  /** Layout for new channel mentions. Existing Discord threads remain threads. */
  conversationMode?: "thread" | "inline_reply";
  discordApiUrl?: string;
  fetch?: DiscordbotFetch;
  guildAllowlist?: readonly string[];
  idleTimeoutMs?: number;
  /** Liveness probe for `/health`; reflects the Gateway connection state. */
  isGatewayActive?: () => boolean;
  logger?: Logger;
  mapper?: CodexAppServerToChatStreamOptions;
  /** Discord delta: per-guild cap on concurrently executing runs. Default 3. */
  maxConcurrentExecutionsPerGuild?: number;
  maxDurationMs?: number;
  mentionRoleIds?: string[];
  /** Rename auto-created threads to the message-derived title. Defaults to true. */
  nameThreads?: boolean;
  postgresUrl?: string;
  /** Public progress surface. `reactions` never posts reasoning or statuses. */
  progressMode?: "narration" | "reactions";
  publicKey: string;
  /** Append model/harness/reasoning metadata to first, every, or no response. */
  responseMetadataMode?: "first" | "always" | "never";
  responseMetadataHarness?: string;
  responseMetadataModel?: string;
  responseMetadataReasoning?: string;
  /** Include end-to-end turn latency in a rendered response metadata footer. */
  responseLatencyEnabled?: boolean;
  recoverRenderObligationsOnStart?: boolean;
  /** Initial exponential backoff for render recovery. Default 250ms. */
  renderRetryInitialDelayMs?: number;
  /** Maximum render recovery backoff. Default 5s. */
  renderRetryMaxDelayMs?: number;
  resolveVisibleChannelIds?: (input: {
    currentChannelId: string;
    currentThreadId?: string;
    guildId: string;
    userId: string;
  }) => Promise<string[]>;
  state?: StateAdapter;
  stateKeyPrefix?: string;
  /**
   * Discord delta (mirrors slackbotv2's `triggerBotAllowlist`): bot user ids
   * whose messages may trigger/append despite being bot-authored.
   */
  triggerBotAllowlist?: readonly string[];
  userName?: string;
};

export type Discordbot = {
  app: Hono;
  chat: Chat;
  adapter: GatewayCapableAdapter;
  /** Wait until the durable state backend is available before accepting ingress. */
  ready: () => Promise<void>;
};

export type DiscordbotThreadState = {
  activeExecution?: boolean;
  /**
   * Discord delta: epoch ms when `activeExecution` was last (re)confirmed;
   * the flag is ignored once this is older than the active-execution TTL.
   * Cleared (null) together with the flag.
   */
  activeExecutionStartedAt?: number | null;
  executedMessageIds?: string[];
  forwardedMessageIds?: string[];
  historyForwarded?: boolean;
  lastEventId?: number;
  renderObligation?: DiscordbotRenderObligation | null;
};

export type DiscordbotRenderObligation = {
  afterEventId: number;
  executionId: string;
  message: DiscordbotApiMessage;
};

export type DiscordbotMessageMode = "append" | "execute";

export type DiscordbotRendererSource = RustSessionStreamEvent | JsonObject;

export type DiscordbotTrace = {
  includeContext: boolean;
  messageId: string;
  mode: DiscordbotMessageMode;
  openStream: boolean;
  startedAtMs: number;
  threadId: string;
};

export type ForwardSessionInput = {
  /** Acting member for a new execution; omitted for append-only steering. */
  actorUserId?: string;
  afterEventId: number;
  /**
   * Human-readable channel name carried in the create-session metadata as
   * `discord_conversation_name`; api-rs uses it as the session principal's
   * display name.
   */
  conversationName?: string;
  executionId?: string;
  executeMessage?: DiscordbotApiMessage;
  messages: DiscordbotApiMessage[];
  onEventId(eventId: number): void;
  openStream: boolean;
  threadId: string;
  trace?: DiscordbotTrace;
  visibleChannelIds?: string[];
};

/** Minimal slice of the Discord adapter the Gateway runner needs. */
export type GatewayCapableAdapter = {
  startGatewayListener(
    options: { waitUntil(promise: Promise<unknown>): void },
    durationMs?: number,
    abortSignal?: AbortSignal,
    webhookUrl?: string,
  ): Promise<unknown>;
};

/** Minimal slice of the Discord adapter used to send a typing indicator. */
export type TypingCapableAdapter = {
  startTyping?(threadId: string, status?: string): Promise<void>;
};
