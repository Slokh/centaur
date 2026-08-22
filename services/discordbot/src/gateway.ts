import type { Chat, Logger } from "chat";
import type { GatewayCapableAdapter } from "./types";

/**
 * `startGatewayListener` treats `durationMs` as a self-destruct timer backed by a single
 * `setTimeout`; within that window discord.js maintains one Gateway session with native RESUME,
 * so a large value gives us one long-lived connection rather than a re-IDENTIFY loop (which would
 * burn the 1000/24h IDENTIFY budget). If the connection ends before this elapses it's a
 * fatal/login error and we let the process exit so Kubernetes restarts the pod.
 *
 * This is capped at the maximum delay a 32-bit `setTimeout` can represent (2^31-1 ms ≈ 24.8 days).
 * A larger value (e.g. one year) silently overflows and clamps to 1ms, firing the self-destruct
 * almost immediately and crash-looping the pod. At ~24.8 days the timer forces at most one
 * reconnect/IDENTIFY per window — negligible against the 1000/24h budget.
 */
const LONG_RUNNING_MS = 2_147_483_647;

// Discord delta (no slackbotv2 analog): discord.js can sit in a RESUME loop
// for a long time without the listener promise settling, so `/health` also
// needs to reflect transient connection state. The patched adapter's status
// callback drives this timestamp, and `isActive()` goes false after the
// gateway has been down for >60s.
const GATEWAY_DISCONNECT_STALE_MS = 60_000;
const GATEWAY_STARTUP_GRACE_MS = 120_000;
const GATEWAY_WATCHDOG_INTERVAL_MS = 15_000;

let gatewayDisconnectedAtMs: number | null = null;
let gatewayConnected = false;
let gatewayHasConnected = false;

/** Records a Gateway connect/disconnect transition (timestamp-based). */
export function setGatewayConnected(
  connected: boolean,
  atEpochMs = Date.now(),
): void {
  if (connected) {
    gatewayConnected = true;
    gatewayHasConnected = true;
    gatewayDisconnectedAtMs = null;
    return;
  }
  gatewayConnected = false;
  // Keep the FIRST disconnect timestamp so repeated disconnect signals
  // don't push the staleness window forward.
  gatewayDisconnectedAtMs ??= atEpochMs;
}

/** True until the gateway has been disconnected for more than the stale window. */
export function isGatewayConnectionFresh(nowEpochMs = Date.now()): boolean {
  if (gatewayConnected) return true;
  return gatewayDisconnectedAtMs !== null &&
    nowEpochMs - gatewayDisconnectedAtMs <= GATEWAY_DISCONNECT_STALE_MS;
}

export function isGatewayConnected(): boolean {
  return gatewayConnected;
}

export type GatewayController = {
  /** True once the listener has started and the connection has not ended. */
  isActive(): boolean;
  /** Run the stale-connection watchdog immediately. Exposed for deterministic tests. */
  checkHealth(nowEpochMs?: number): void;
  /** Initialize the chat instance and open the single long-lived Gateway connection. */
  start(chat: Chat, adapter: GatewayCapableAdapter): Promise<void>;
  /** Stop accepting Gateway work and wait for the connection to close. */
  shutdown(): Promise<void>;
};

type GatewayControllerDeps = {
  logger: Logger;
  /** Override for tests — defaults to `process.exit`. */
  onFatalEnd?: () => void;
};

export function createGatewayController(
  deps: GatewayControllerDeps,
): GatewayController {
  const { logger } = deps;
  const onFatalEnd = deps.onFatalEnd ?? (() => process.exit(1));
  const abort = new AbortController();
  let active = false;
  let shuttingDown = false;
  let monitor: Promise<void> | undefined;
  let startedAtMs = 0;
  let fatalTriggered = false;
  let watchdog: ReturnType<typeof setInterval> | undefined;

  const triggerFatal = (event: string): void => {
    if (fatalTriggered || shuttingDown) return;
    fatalTriggered = true;
    logger.error(event);
    onFatalEnd();
  };

  const checkHealth = (nowEpochMs = Date.now()): void => {
    if (!active || shuttingDown || isGatewayConnectionFresh(nowEpochMs)) return;
    if (!gatewayHasConnected && nowEpochMs - startedAtMs <= GATEWAY_STARTUP_GRACE_MS) {
      return;
    }
    triggerFatal("discordbot_gateway_stale");
  };

  return {
    isActive: () => active && isGatewayConnected(),
    checkHealth,

    async start(chat, adapter) {
      // Adapters initialize lazily (normally on the first webhook). Direct-mode Gateway
      // processing needs the adapter wired to the chat instance up front.
      await chat.initialize();

      const tracked: Array<Promise<unknown>> = [];
      // Direct mode: no webhookUrl, so MessageCreate is dispatched through Chat in-process.
      await adapter.startGatewayListener(
        {
          waitUntil: (promise) =>
            tracked.push(Promise.resolve(promise).catch(() => undefined)),
        },
        LONG_RUNNING_MS,
        abort.signal,
        undefined,
      );
      active = true;
      startedAtMs = Date.now();
      logger.info("discordbot_gateway_started");
      watchdog = setInterval(checkHealth, GATEWAY_WATCHDOG_INTERVAL_MS);
      watchdog.unref();

      monitor = Promise.all(tracked)
        .then(() => undefined)
        .finally(() => {
          active = false;
          if (shuttingDown) {
            logger.info("discordbot_gateway_stopped");
            return;
          }
          // A single long-lived connection ended on its own — almost always a fatal error
          // (invalid token / disallowed intents). Exit so k8s restarts with backoff.
          triggerFatal("discordbot_gateway_ended_unexpectedly");
        });
    },

    async shutdown() {
      shuttingDown = true;
      if (watchdog) clearInterval(watchdog);
      abort.abort();
      if (monitor) await monitor;
    },
  };
}
