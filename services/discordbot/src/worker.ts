import { createPostgresState } from "@chat-adapter/state-pg";
import { Hono, type Context } from "hono";
import pg from "pg";
import { PostgresDiscordEventSinkOutbox } from "./discord-event-sink-outbox";
import { recoverDiscordEventSink } from "./discord-event-sink";
import { reconcileDiscordArchive } from "./discord-reconciliation";
import type { DiscordbotOptions } from "./types";
import { errorMessage, singleFlight } from "./utils";

const port = numberEnv("PORT", 3002);
const consoleLogger = {
  debug: (message: string, data?: unknown) => log("debug", message, data),
  info: (message: string, data?: unknown) => log("info", message, data),
  warn: (message: string, data?: unknown) => log("warn", message, data),
  error: (message: string, data?: unknown) => log("error", message, data),
  child: () => consoleLogger,
};
const postgresUrl =
  optionalEnv("DISCORDBOT_DATABASE_URL") ??
  optionalEnv("DATABASE_URL") ??
  optionalEnv("POSTGRES_URL");
if (!postgresUrl) {
  throw new Error(
    "DISCORDBOT_DATABASE_URL (or DATABASE_URL / POSTGRES_URL) is required",
  );
}

const stateKeyPrefix = optionalEnv("DISCORDBOT_STATE_KEY_PREFIX");
const statePool = new pg.Pool({
  connectionString: postgresUrl,
  connectionTimeoutMillis: 5_000,
  max: 2,
  query_timeout: 5_000,
});
statePool.on("error", (error) => {
  log("warn", "discordbot_worker_postgres_pool_error", {
    error: errorMessage(error),
  });
});
const state = createPostgresState({
  client: statePool,
  keyPrefix: stateKeyPrefix ?? "centaur-discordbot",
  logger: consoleLogger,
});
const outboxPool = new pg.Pool({
  connectionString: postgresUrl,
  connectionTimeoutMillis: 5_000,
  max: 4,
  query_timeout: 5_000,
});
outboxPool.on("error", (error) => {
  log("warn", "discordbot_worker_outbox_pool_error", {
    error: errorMessage(error),
  });
});
const outbox = new PostgresDiscordEventSinkOutbox({
  keyPrefix: stateKeyPrefix,
  pool: outboxPool,
});

const options: DiscordbotOptions = {
  apiUrl: "",
  applicationId: requiredEnv("DISCORD_APPLICATION_ID"),
  eventSinkDeliveryTimeoutMs: optionalNumberEnv(
    "DISCORDBOT_EVENT_SINK_DELIVERY_TIMEOUT_MS",
  ),
  eventSinkOutbox: outbox,
  eventSinkRecoveryBatchSize: optionalNumberEnv(
    "DISCORDBOT_EVENT_SINK_RECOVERY_BATCH_SIZE",
  ),
  eventSinkRecoveryConcurrency: optionalNumberEnv(
    "DISCORDBOT_EVENT_SINK_RECOVERY_CONCURRENCY",
  ),
  eventSinkRecoveryLeaseMs: optionalNumberEnv(
    "DISCORDBOT_EVENT_SINK_RECOVERY_LEASE_MS",
  ),
  eventSinkToken: requiredEnv(
    "DISCORDBOT_EVENT_SINK_TOKEN",
  ),
  eventSinkUrl: requiredEnv(
    "DISCORDBOT_EVENT_SINK_URL",
  ),
  applicationArchiveReconciliationConcurrency: optionalNumberEnv(
    "DISCORDBOT_EVENT_SINK_ARCHIVE_RECONCILIATION_CONCURRENCY",
  ),
  applicationArchiveReconciliationEnabled:
    optionalEnv("DISCORDBOT_EVENT_SINK_ARCHIVE_RECONCILIATION_ENABLED") ===
    "true",
  applicationArchiveReconciliationIntervalMs: optionalSecondsEnv(
    "DISCORDBOT_EVENT_SINK_ARCHIVE_RECONCILIATION_INTERVAL_SECONDS",
  ),
  botToken: requiredEnv("DISCORD_BOT_TOKEN"),
  discordApiUrl: optionalEnv("DISCORD_API_URL"),
  guildAllowlist: optionalList("DISCORDBOT_GUILD_ALLOWLIST"),
  logger: consoleLogger,
  publicKey: requiredEnv("DISCORD_PUBLIC_KEY"),
  stateKeyPrefix,
};

const recoveryIntervalMs = optionalNumberEnv(
  "DISCORDBOT_EVENT_SINK_RECOVERY_INTERVAL_MS",
) ?? 1_000;
const archiveBacklogHighWatermark = optionalNumberEnv(
  "DISCORDBOT_EVENT_SINK_ARCHIVE_BACKLOG_HIGH_WATERMARK",
) ?? 10_000;

let durableStateReady = false;
let shuttingDown = false;
let recoveryTimer: ReturnType<typeof setInterval> | undefined;
let archiveTimer: ReturnType<typeof setInterval> | undefined;
let recoveryWork: Promise<void> = Promise.resolve();
let archiveWork: Promise<void> = Promise.resolve();

const recoverOutbox = singleFlight(async () => {
  try {
    const result = await recoverDiscordEventSink(options, outbox);
    if (result.claimed > 0 || result.failed > 0) {
      log("info", "discordbot_event_sink_recovered", result);
    }
  } catch (error) {
    log("error", "discordbot_event_sink_recovery_failed", {
      error: errorMessage(error),
    });
  }
});

const reconcileArchive = singleFlight(async () => {
  try {
    const pending = await outbox.pendingCount();
    if (pending >= archiveBacklogHighWatermark) {
      log("warn", "discordbot_application_archive_backpressured", {
        pending_count: pending,
        high_watermark: archiveBacklogHighWatermark,
      });
      return;
    }
    const observed = await reconcileDiscordArchive(options, state);
    if (observed > 0) {
      log("info", "discordbot_application_archive_reconciled", {
        observed_count: observed,
      });
    }
  } catch (error) {
    log("error", "discordbot_application_archive_reconciliation_failed", {
      error: errorMessage(error),
    });
  }
});

const triggerRecovery = (): void => {
  recoveryWork = recoverOutbox();
};

const triggerArchive = (): void => {
  archiveWork = reconcileArchive();
};

const app = new Hono();
app.get("/live", (c) =>
  c.json({ ok: true, service: "discordbot-event-sink-worker" }),
);
const readiness = async (c: Context) => {
  let stateReady = durableStateReady;
  let deadLetterCount = 0;
  if (stateReady) {
    try {
      const [, , observedDeadLetterCount] = await Promise.all([
        state.get("discordbot:worker:health"),
        outbox.healthCheck(),
        outbox.deadLetterCount(),
      ]);
      deadLetterCount = observedDeadLetterCount;
    } catch {
      stateReady = false;
    }
  }
  return c.json(
    {
      ok: stateReady,
      service: "discordbot-event-sink-worker",
      state: stateReady,
      dead_letter_count: deadLetterCount,
    },
    stateReady ? 200 : 503,
  );
};
app.get("/ready", readiness);
app.get("/health", readiness);
const server = Bun.serve({ port, fetch: app.fetch });
log("info", "discordbot_event_sink_worker_started", { port: server.port });

await connectDurableState();
durableStateReady = true;
triggerRecovery();
recoveryTimer = setInterval(triggerRecovery, recoveryIntervalMs);
recoveryTimer.unref();

if (options.applicationArchiveReconciliationEnabled === true) {
  triggerArchive();
  archiveTimer = setInterval(
    triggerArchive,
    Math.max(
      1_000,
      options.applicationArchiveReconciliationIntervalMs ?? 60_000,
    ),
  );
  archiveTimer.unref();
}

const shutdown = async (signal: string): Promise<void> => {
  if (shuttingDown) return;
  shuttingDown = true;
  durableStateReady = false;
  log("info", "discordbot_event_sink_worker_shutdown_started", { signal });
  if (recoveryTimer) clearInterval(recoveryTimer);
  if (archiveTimer) clearInterval(archiveTimer);
  await Promise.allSettled([recoveryWork, archiveWork]);
  await Promise.allSettled([state.disconnect(), outbox.disconnect()]);
  server.stop();
  log("info", "discordbot_event_sink_worker_shutdown_complete", { signal });
  process.exit(0);
};
process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));

async function connectDurableState(): Promise<void> {
  for (let attempt = 0; ; attempt++) {
    try {
      await Promise.all([state.connect(), outbox.connect()]);
      return;
    } catch (error) {
      const delayMs = Math.min(250 * 2 ** attempt, 10_000);
      log("warn", "discordbot_event_sink_worker_postgres_connect_retry", {
        attempt: attempt + 1,
        delay_ms: delayMs,
        error: errorMessage(error),
      });
      await Bun.sleep(delayMs);
    }
  }
}

function optionalEnv(name: string): string | undefined {
  const value = process.env[name]?.trim();
  return value ? value : undefined;
}

function optionalList(name: string): string[] | undefined {
  const value = optionalEnv(name);
  if (!value) return undefined;
  return value.split(/[\s,]+/).map((part) => part.trim()).filter(Boolean);
}

function requiredEnv(name: string): string {
  const value = optionalEnv(name);
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function numberEnv(name: string, fallback: number): number {
  return optionalNumberEnv(name) ?? fallback;
}

function optionalNumberEnv(name: string): number | undefined {
  const value = optionalEnv(name);
  if (!value) return undefined;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

function optionalSecondsEnv(name: string): number | undefined {
  const seconds = optionalNumberEnv(name);
  return seconds === undefined ? undefined : seconds * 1_000;
}

function log(level: string, message: string, data?: unknown): void {
  console.log(JSON.stringify({
    level,
    service: "discordbot-event-sink-worker",
    timestamp: new Date().toISOString(),
    event: message,
    ...(data && typeof data === "object"
      ? data as Record<string, unknown>
      : {}),
  }));
}
