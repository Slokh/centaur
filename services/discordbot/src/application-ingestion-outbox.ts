import { randomUUID } from "node:crypto";
import type pg from "pg";

const DEFAULT_MAX_PAYLOAD_BYTES = 2 * 1024 * 1024;

export type ApplicationIngestionClaim = {
  attempt: number;
  leaseToken: string;
  payload: Record<string, unknown>;
  sourceKey: string;
};

export interface ApplicationIngestionOutbox {
  acknowledge(claim: ApplicationIngestionClaim): Promise<boolean>;
  claim(batchSize: number, leaseMs: number): Promise<ApplicationIngestionClaim[]>;
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  enqueue(sourceKey: string, payload: Record<string, unknown>): Promise<void>;
  healthCheck(): Promise<void>;
  pendingCount(): Promise<number>;
  retry(
    claim: ApplicationIngestionClaim,
    delayMs: number,
    error: string,
  ): Promise<boolean>;
}

type QueryablePool = Pick<pg.Pool, "connect" | "end" | "query">;

type PostgresOutboxOptions = {
  keyPrefix?: string;
  maxPayloadBytes?: number;
  pool: QueryablePool;
};

/**
 * A pending-only, lease-based application delivery queue. Successful rows are
 * deleted, so recovery cost follows outstanding work instead of all historical
 * Discord observations.
 */
export class PostgresApplicationIngestionOutbox
implements ApplicationIngestionOutbox {
  private readonly keyPrefix: string;
  private readonly maxPayloadBytes: number;
  private readonly pool: QueryablePool;
  private connected = false;

  constructor(options: PostgresOutboxOptions) {
    this.keyPrefix = options.keyPrefix ?? "centaur-discordbot";
    this.maxPayloadBytes = options.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES;
    this.pool = options.pool;
  }

  async connect(): Promise<void> {
    if (this.connected) return;
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        ["centaur:discord-application-ingestion-outbox:schema"],
      );
      await client.query(`
        CREATE TABLE IF NOT EXISTS discord_application_ingestion_outbox (
          key_prefix TEXT NOT NULL,
          source_key TEXT NOT NULL,
          payload JSONB NOT NULL,
          attempt_count INTEGER NOT NULL DEFAULT 0,
          available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          leased_until TIMESTAMPTZ,
          lease_token TEXT,
          last_error TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          PRIMARY KEY (key_prefix, source_key)
        )
      `);
      await client.query(`
        CREATE INDEX IF NOT EXISTS discord_application_ingestion_outbox_ready_idx
        ON discord_application_ingestion_outbox
          (key_prefix, available_at, created_at)
      `);
      await client.query("COMMIT");
      this.connected = true;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async disconnect(): Promise<void> {
    this.connected = false;
    await this.pool.end();
  }

  async enqueue(
    sourceKey: string,
    payload: Record<string, unknown>,
  ): Promise<void> {
    if (!sourceKey) throw new Error("application ingestion event lacks source_key");
    const serialized = JSON.stringify(payload);
    if (Buffer.byteLength(serialized) > this.maxPayloadBytes) {
      throw new Error("application ingestion payload exceeds durable queue limit");
    }
    await this.pool.query(
      `
        INSERT INTO discord_application_ingestion_outbox
          (key_prefix, source_key, payload)
        VALUES ($1, $2, $3::jsonb)
        ON CONFLICT (key_prefix, source_key) DO NOTHING
      `,
      [this.keyPrefix, sourceKey, serialized],
    );
  }

  async healthCheck(): Promise<void> {
    await this.pool.query("SELECT 1");
  }

  async claim(
    batchSize: number,
    leaseMs: number,
  ): Promise<ApplicationIngestionClaim[]> {
    if (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > 1_000) {
      throw new Error("application ingestion batch size must be between 1 and 1000");
    }
    if (!Number.isInteger(leaseMs) || leaseMs < 1_000) {
      throw new Error("application ingestion lease must be at least one second");
    }
    const leaseToken = randomUUID();
    const result = await this.pool.query<{
      attempt_count: number;
      payload: Record<string, unknown>;
      source_key: string;
    }>(
      `
        WITH candidates AS (
          SELECT source_key
          FROM discord_application_ingestion_outbox
          WHERE key_prefix = $1
            AND available_at <= NOW()
            AND (leased_until IS NULL OR leased_until <= NOW())
          ORDER BY available_at, created_at
          LIMIT $2
          FOR UPDATE SKIP LOCKED
        )
        UPDATE discord_application_ingestion_outbox AS outbox
        SET attempt_count = LEAST(outbox.attempt_count + 1, 1000000),
            leased_until = NOW() + ($3::bigint * INTERVAL '1 millisecond'),
            lease_token = $4,
            updated_at = NOW()
        FROM candidates
        WHERE outbox.key_prefix = $1
          AND outbox.source_key = candidates.source_key
        RETURNING outbox.source_key, outbox.payload, outbox.attempt_count
      `,
      [this.keyPrefix, batchSize, leaseMs, leaseToken],
    );
    return result.rows.map((row) => ({
      attempt: row.attempt_count,
      leaseToken,
      payload: row.payload,
      sourceKey: row.source_key,
    }));
  }

  async acknowledge(claim: ApplicationIngestionClaim): Promise<boolean> {
    const result = await this.pool.query(
      `
        DELETE FROM discord_application_ingestion_outbox
        WHERE key_prefix = $1 AND source_key = $2 AND lease_token = $3
      `,
      [this.keyPrefix, claim.sourceKey, claim.leaseToken],
    );
    return result.rowCount === 1;
  }

  async retry(
    claim: ApplicationIngestionClaim,
    delayMs: number,
    error: string,
  ): Promise<boolean> {
    const boundedDelayMs = Math.max(1_000, Math.min(delayMs, 15 * 60_000));
    const result = await this.pool.query(
      `
        UPDATE discord_application_ingestion_outbox
        SET available_at = NOW() + ($4::bigint * INTERVAL '1 millisecond'),
            leased_until = NULL,
            lease_token = NULL,
            last_error = LEFT($5, 500),
            updated_at = NOW()
        WHERE key_prefix = $1 AND source_key = $2 AND lease_token = $3
      `,
      [
        this.keyPrefix,
        claim.sourceKey,
        claim.leaseToken,
        boundedDelayMs,
        error,
      ],
    );
    return result.rowCount === 1;
  }

  async pendingCount(): Promise<number> {
    const result = await this.pool.query<{ count: string }>(
      `
        SELECT COUNT(*)::text AS count
        FROM discord_application_ingestion_outbox
        WHERE key_prefix = $1
      `,
      [this.keyPrefix],
    );
    return Number.parseInt(result.rows[0]?.count ?? "0", 10);
  }
}

export class MemoryApplicationIngestionOutbox
implements ApplicationIngestionOutbox {
  private readonly rows = new Map<string, {
    attempt: number;
    availableAt: number;
    leaseToken?: string;
    leasedUntil?: number;
    payload: Record<string, unknown>;
  }>();

  async connect(): Promise<void> {}
  async disconnect(): Promise<void> {}
  async healthCheck(): Promise<void> {}

  async enqueue(
    sourceKey: string,
    payload: Record<string, unknown>,
  ): Promise<void> {
    if (!this.rows.has(sourceKey)) {
      this.rows.set(sourceKey, { attempt: 0, availableAt: Date.now(), payload });
    }
  }

  async claim(
    batchSize: number,
    leaseMs: number,
  ): Promise<ApplicationIngestionClaim[]> {
    const now = Date.now();
    const claims: ApplicationIngestionClaim[] = [];
    for (const [sourceKey, row] of this.rows) {
      if (claims.length >= batchSize) break;
      if (row.availableAt > now || (row.leasedUntil ?? 0) > now) continue;
      row.attempt += 1;
      row.leaseToken = randomUUID();
      row.leasedUntil = now + leaseMs;
      claims.push({
        attempt: row.attempt,
        leaseToken: row.leaseToken,
        payload: row.payload,
        sourceKey,
      });
    }
    return claims;
  }

  async acknowledge(claim: ApplicationIngestionClaim): Promise<boolean> {
    const row = this.rows.get(claim.sourceKey);
    if (row?.leaseToken !== claim.leaseToken) return false;
    return this.rows.delete(claim.sourceKey);
  }

  async retry(
    claim: ApplicationIngestionClaim,
    delayMs: number,
    _error: string,
  ): Promise<boolean> {
    const row = this.rows.get(claim.sourceKey);
    if (row?.leaseToken !== claim.leaseToken) return false;
    row.availableAt = Date.now() + delayMs;
    row.leasedUntil = undefined;
    row.leaseToken = undefined;
    return true;
  }

  async pendingCount(): Promise<number> {
    return this.rows.size;
  }
}
