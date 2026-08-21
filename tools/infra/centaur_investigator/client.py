"""Centaur readonly PostgreSQL investigation helper."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse, urlunparse

import asyncpg
import httpx

from centaur_sdk import secret

CENTAUR_POSTGRES_DSN_ENV = "CENTAUR_POSTGRES_DSN"
CENTAUR_INVESTIGATOR_DATABASE_ENV = "CENTAUR_INVESTIGATOR_POSTGRES_DATABASE"
DEFAULT_POSTGRES_DATABASE = "ai_v2"
DEFAULT_LIMIT = 25
MAX_LIMIT = 200
DEFAULT_WINDOW_HOURS = 24
MAX_WINDOW_HOURS = 24 * 30
MAX_LOG_LIMIT = 500
DISCORD_BOT_TOKEN_ENV = "DISCORD_BOT_TOKEN"
DISCORD_API_URL = "https://discord.com/api/v10"
DISCORD_EPOCH_MS = 1_420_070_400_000

_SLACK_URL_RE = re.compile(r"https?://[^\s<>|]+/archives/[A-Z0-9]+/p\d{10,20}[^\s<>|]*")
_SLACK_THREAD_KEY_RE = re.compile(
    r"\b(?P<thread_key>[A-Za-z][A-Za-z0-9_.-]*:"
    r"(?:(?P<team>T[A-Z0-9]+):)?(?P<channel>[CDG][A-Z0-9]+):"
    r"(?P<thread_ts>\d{10}\.\d{1,6}))\b"
)
_CHANNEL_TS_RE = re.compile(r"\b(?P<channel>[CDG][A-Z0-9]+):(?P<thread_ts>\d{10}\.\d{1,6})\b")
_KEY_SOURCE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]*:")
_DISCORD_URL_RE = re.compile(
    r"https?://(?:(?:canary|ptb)\.)?discord(?:app)?\.com/channels/"
    r"(?P<guild>@me|\d+)/(?P<channel>\d+)/(?P<message>\d+)"
)
_DISCORD_THREAD_KEY_RE = re.compile(
    r"\bdiscord:(?P<guild>@me|\d+):(?P<channel>\d+)"
    r"(?::(?P<destination>reply~\d+|\d+))?\b"
)

_SAFE_DISCORD_LOG_FIELDS = {
    "_time",
    "timestamp",
    "service",
    "level",
    "event",
    "status",
    "success",
    "duration_ms",
    "elapsed_ms",
    "phase_ms",
    "latency_ms",
    "retry_after_ms",
    "execution_id",
    "sandbox_id",
    "sandbox_ready_source",
    "sandbox_ready_duration_ms",
    "message_id",
    "messageId",
    "channel_id",
    "channelId",
    "guild_id",
    "guildId",
    "tool_name",
    "tool_method",
    "tool_args_count",
    "exit_code",
    "completion_reason",
}


def _clamp(value: int, *, minimum: int, maximum: int) -> int:
    return max(minimum, min(maximum, int(value)))


def _scoped_database_url() -> str:
    value = os.getenv(CENTAUR_POSTGRES_DSN_ENV)  # noqa: TID251
    if value is None:
        value = secret(CENTAUR_POSTGRES_DSN_ENV, default="")
    value = value.strip()
    if value == CENTAUR_POSTGRES_DSN_ENV:
        return ""
    return value


def _database_url_with_name(value: str, database: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme and parsed.netloc and parsed.path in ("", "/"):
        return urlunparse(parsed._replace(path=f"/{database}"))
    return value


def _postgres_database_name() -> str:
    value = os.getenv(CENTAUR_INVESTIGATOR_DATABASE_ENV, DEFAULT_POSTGRES_DATABASE)  # noqa: TID251
    return value.strip() or DEFAULT_POSTGRES_DATABASE


def _isoformat(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    except ValueError:
        return None


def _serialize(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (dict, list, str, int, float, bool)) or value is None:
        return value
    try:
        return json.loads(json.dumps(value))
    except TypeError:
        return str(value)


def _record_to_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "items"):
        return {key: _serialize(value) for key, value in row.items()}
    raise TypeError(f"expected a mapping-like database row, got {type(row).__name__}")


def _connection_role(connection: dict[str, Any]) -> str | None:
    row = connection.get("row") if isinstance(connection, dict) else None
    if not isinstance(row, dict):
        return None
    active_role = str(row.get("active_role") or "").strip()
    if active_role and active_role.lower() != "none":
        return active_role
    return str(row.get("current_user") or "") or None


def _normalize_ts(value: str | None) -> str | None:
    if not value:
        return None
    text = unquote(str(value)).strip()
    if not text:
        return None
    if "." in text:
        left, right = text.split(".", 1)
        if left.isdigit() and right.isdigit():
            return f"{left}.{right[:6].ljust(6, '0')}"
        return None
    digits = re.sub(r"\D", "", text)
    if len(digits) <= 10:
        return None
    return f"{digits[:10]}.{digits[10:16].ljust(6, '0')}"


def _slack_ts_to_datetime(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(float(ts), tz=UTC)
    except (TypeError, ValueError, OSError):
        return None


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _log_field_expr(field: str, value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'{field}:"{escaped}"'


def _thread_key_candidates(
    *,
    channel_id: str,
    thread_ts: str,
    team_id: str | None = None,
    source: str = "slack",
) -> list[str]:
    candidates = []
    if team_id:
        candidates.extend(
            [
                f"{source}:{team_id}:{channel_id}:{thread_ts}",
                f"slack:{team_id}:{channel_id}:{thread_ts}",
                f"chat:{team_id}:{channel_id}:{thread_ts}",
            ]
        )
    candidates.extend(
        [
            f"{source}:{channel_id}:{thread_ts}",
            f"slack:{channel_id}:{thread_ts}",
            f"chat:{channel_id}:{thread_ts}",
        ]
    )
    return _dedupe(candidates)


def _first_qs(query: dict[str, list[str]], *names: str) -> str | None:
    for name in names:
        values = query.get(name)
        if values:
            return values[0]
    return None


def _clean_reference_text(reference: str) -> str:
    text = reference.strip()
    if text.startswith("<") and ">" in text:
        text = text[1 : text.index(">")]
    if "|" in text and text.startswith("http"):
        text = text.split("|", 1)[0]
    return text.strip()


def parse_slack_reference(reference: str) -> dict[str, Any]:
    """Parse a Slack permalink or Centaur thread key into identifiers only."""
    text = _clean_reference_text(reference)
    direct = _SLACK_THREAD_KEY_RE.search(text)
    if direct:
        thread_key = direct.group("thread_key")
        channel_id = direct.group("channel")
        team_id = direct.group("team")
        thread_ts = _normalize_ts(direct.group("thread_ts"))
        if not thread_ts:
            return {"status": "error", "error": "invalid thread timestamp"}
        source = thread_key.split(":", 1)[0]
        return {
            "status": "ok",
            "input": reference,
            "kind": "thread_key",
            "source": source,
            "team_id": team_id,
            "channel_id": channel_id,
            "message_ts": thread_ts,
            "thread_ts": thread_ts,
            "thread_datetime": _isoformat(_slack_ts_to_datetime(thread_ts)),
            "thread_key": thread_key,
            "thread_key_candidates": _thread_key_candidates(
                channel_id=channel_id,
                thread_ts=thread_ts,
                team_id=team_id,
                source=source,
            ),
            "thread_key_like": f"%:{channel_id}:{thread_ts}",
            "channel_key_like": f"%:{channel_id}:%",
        }

    channel_ts = _CHANNEL_TS_RE.search(text)
    if channel_ts:
        channel_id = channel_ts.group("channel")
        thread_ts = _normalize_ts(channel_ts.group("thread_ts"))
        if thread_ts:
            return {
                "status": "ok",
                "input": reference,
                "kind": "channel_ts",
                "source": "slack",
                "team_id": None,
                "channel_id": channel_id,
                "message_ts": thread_ts,
                "thread_ts": thread_ts,
                "thread_datetime": _isoformat(_slack_ts_to_datetime(thread_ts)),
                "thread_key": f"slack:{channel_id}:{thread_ts}",
                "thread_key_candidates": _thread_key_candidates(
                    channel_id=channel_id,
                    thread_ts=thread_ts,
                ),
                "thread_key_like": f"%:{channel_id}:{thread_ts}",
                "channel_key_like": f"%:{channel_id}:%",
            }

    url_match = _SLACK_URL_RE.search(text)
    if not url_match and text.startswith(("http://", "https://", "slack://")):
        url = text
    elif url_match:
        url = url_match.group(0)
    else:
        return {"status": "error", "error": "no Slack permalink or thread_key found"}

    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    team_id = _first_qs(query, "team", "team_id")
    channel_id = _first_qs(query, "cid", "channel", "channel_id", "id")
    message_ts = _normalize_ts(_first_qs(query, "message", "ts"))

    path_match = re.search(r"/archives/(?P<channel>[A-Z0-9]+)/p(?P<ts>\d+)", parsed.path)
    if path_match:
        channel_id = channel_id or path_match.group("channel")
        message_ts = message_ts or _normalize_ts(path_match.group("ts"))

    thread_ts = _normalize_ts(_first_qs(query, "thread_ts")) or message_ts
    if parsed.scheme == "slack":
        channel_id = channel_id or _first_qs(query, "id")
        thread_ts = _normalize_ts(_first_qs(query, "thread_ts", "message", "ts")) or thread_ts
        message_ts = message_ts or _normalize_ts(_first_qs(query, "message", "ts"))

    if not channel_id or not thread_ts:
        return {"status": "error", "error": "could not parse Slack channel and thread timestamp"}

    message_ts = message_ts or thread_ts
    return {
        "status": "ok",
        "input": reference,
        "kind": "slack_permalink",
        "source": "slack",
        "team_id": team_id,
        "channel_id": channel_id,
        "message_ts": message_ts,
        "thread_ts": thread_ts,
        "thread_datetime": _isoformat(_slack_ts_to_datetime(thread_ts)),
        "message_datetime": _isoformat(_slack_ts_to_datetime(message_ts)),
        "thread_key": f"slack:{channel_id}:{thread_ts}",
        "thread_key_candidates": _thread_key_candidates(
            channel_id=channel_id,
            thread_ts=thread_ts,
            team_id=team_id,
        ),
        "thread_key_like": f"%:{channel_id}:{thread_ts}",
        "channel_key_like": f"%:{channel_id}:%",
        "permalink": f"https://slack.com/archives/{channel_id}/p{message_ts.replace('.', '')}",
    }


def _discord_snowflake_datetime(snowflake: str | None) -> datetime | None:
    if not snowflake or not str(snowflake).isdigit():
        return None
    try:
        milliseconds = (int(snowflake) >> 22) + DISCORD_EPOCH_MS
        return datetime.fromtimestamp(milliseconds / 1000, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None


def _discord_thread_key_candidates(
    *,
    guild_id: str,
    channel_id: str,
    message_id: str | None,
    parent_channel_id: str | None = None,
) -> list[str]:
    candidates: list[str] = []
    if parent_channel_id and parent_channel_id != channel_id:
        candidates.append(f"discord:{guild_id}:{parent_channel_id}:{channel_id}")
    if message_id:
        candidates.append(f"discord:{guild_id}:{channel_id}:reply~{message_id}")
    candidates.append(f"discord:{guild_id}:{channel_id}")
    return _dedupe(candidates)


def parse_discord_reference(reference: str) -> dict[str, Any]:
    """Parse a Discord message permalink or Discord Centaur thread key."""
    text = _clean_reference_text(reference)
    direct = _DISCORD_THREAD_KEY_RE.search(text)
    if direct:
        guild_id = direct.group("guild")
        channel_id = direct.group("channel")
        destination = direct.group("destination")
        message_id = destination[6:] if destination and destination.startswith("reply~") else None
        thread_id = destination if destination and not destination.startswith("reply~") else None
        thread_key = direct.group(0)
        message_dt = _discord_snowflake_datetime(message_id)
        return {
            "status": "ok",
            "input": reference,
            "kind": "thread_key",
            "source": "discord",
            "guild_id": guild_id,
            "channel_id": channel_id,
            "thread_id": thread_id,
            "message_id": message_id,
            "source_message_id": message_id,
            "message_datetime": _isoformat(message_dt),
            "event_datetime": _isoformat(message_dt),
            "thread_key": thread_key,
            "thread_key_candidates": [thread_key],
            "thread_key_like": None,
            "channel_key_like": f"discord:{guild_id}:{channel_id}%",
        }

    match = _DISCORD_URL_RE.search(text)
    if not match:
        return {"status": "error", "error": "no Discord message permalink or thread_key found"}

    guild_id = match.group("guild")
    channel_id = match.group("channel")
    message_id = match.group("message")
    message_dt = _discord_snowflake_datetime(message_id)
    permalink = f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"
    return {
        "status": "ok",
        "input": reference,
        "kind": "discord_permalink",
        "source": "discord",
        "guild_id": guild_id,
        "channel_id": channel_id,
        "thread_id": None,
        "message_id": message_id,
        "source_message_id": message_id,
        "message_datetime": _isoformat(message_dt),
        "event_datetime": _isoformat(message_dt),
        "thread_key": f"discord:{guild_id}:{channel_id}:reply~{message_id}",
        "thread_key_candidates": _discord_thread_key_candidates(
            guild_id=guild_id,
            channel_id=channel_id,
            message_id=message_id,
        ),
        "thread_key_like": None,
        "channel_key_like": f"discord:{guild_id}:{channel_id}%",
        "permalink": permalink,
    }


def _safe_discord_log_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Keep diagnostic timing fields while excluding content, arguments, and errors."""
    return {
        key: _serialize(value) for key, value in entry.items() if key in _SAFE_DISCORD_LOG_FIELDS
    }


def _discord_author(message: dict[str, Any]) -> dict[str, Any]:
    author = message.get("author") if isinstance(message.get("author"), dict) else {}
    return {
        "id": str(author.get("id") or ""),
        "username": str(author.get("username") or ""),
        "global_name": author.get("global_name"),
        "bot": bool(author.get("bot")),
    }


def _sanitize_discord_message(message: dict[str, Any], *, include_content: bool) -> dict[str, Any]:
    reference = (
        message.get("message_reference")
        if isinstance(message.get("message_reference"), dict)
        else {}
    )
    reactions = message.get("reactions") if isinstance(message.get("reactions"), list) else []
    attachments = message.get("attachments") if isinstance(message.get("attachments"), list) else []
    result: dict[str, Any] = {
        "id": str(message.get("id") or ""),
        "channel_id": str(message.get("channel_id") or ""),
        "guild_id": str(message.get("guild_id") or ""),
        "author": _discord_author(message),
        "timestamp": message.get("timestamp"),
        "edited_timestamp": message.get("edited_timestamp"),
        "referenced_message_id": reference.get("message_id"),
        "reaction_count": sum(int(row.get("count") or 0) for row in reactions),
        "reactions": [
            {
                "emoji": str((row.get("emoji") or {}).get("name") or ""),
                "count": int(row.get("count") or 0),
            }
            for row in reactions
            if isinstance(row, dict) and isinstance(row.get("emoji"), dict)
        ],
        "attachments": [
            {
                "id": str(row.get("id") or ""),
                "filename": str(row.get("filename") or ""),
                "content_type": row.get("content_type"),
                "size": row.get("size"),
            }
            for row in attachments
            if isinstance(row, dict)
        ],
        "embed_count": len(message.get("embeds") or []),
    }
    if include_content:
        result["content"] = str(message.get("content") or "")
    return result


def _safe_load_module(module_name: str, path: Path) -> Any | None:
    if not path.exists():
        return None
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class CentaurInvestigatorClient:
    """Investigate Centaur state through readonly Postgres access."""

    def __init__(
        self,
        database_url: str | None = None,
        *,
        discord_token: str | None = None,
        discord_http_client: httpx.Client | None = None,
    ) -> None:
        self._database_url = (database_url or _scoped_database_url()).strip()
        self._discord_token = discord_token
        self._discord_http_client = discord_http_client
        self._owns_discord_http_client = False

    def close(self) -> None:
        if self._owns_discord_http_client and self._discord_http_client is not None:
            self._discord_http_client.close()
        self._discord_http_client = None
        self._owns_discord_http_client = False

    def _discord_client(self) -> httpx.Client:
        if self._discord_http_client is None:
            self._discord_http_client = httpx.Client(
                base_url=DISCORD_API_URL,
                timeout=30,
                headers={
                    "User-Agent": "DiscordBot (https://github.com/paradigmxyz/centaur, 0.1.0)"
                },
            )
            self._owns_discord_http_client = True
        return self._discord_http_client

    def _discord_authorization(self) -> str:
        token = (self._discord_token or secret(DISCORD_BOT_TOKEN_ENV, default="")).strip()
        if not token:
            raise RuntimeError(f"{DISCORD_BOT_TOKEN_ENV} is required for Discord permalink lookup")
        # The sandbox proxy replaces the declared placeholder header with
        # ``Bot <real token>``. Direct operator use may provide either a raw bot
        # token or a fully formatted Authorization value.
        if token == DISCORD_BOT_TOKEN_ENV or token.startswith(("Bot ", "Bearer ")):
            return token
        return f"Bot {token}"

    def _discord_get(self, path: str) -> dict[str, Any]:
        response = self._discord_client().get(
            path,
            headers={"Authorization": self._discord_authorization()},
        )
        if response.status_code >= 400:
            code: Any = None
            try:
                payload = response.json()
                code = payload.get("code") if isinstance(payload, dict) else None
            except Exception:
                code = None
            suffix = f", code {code}" if code is not None else ""
            raise RuntimeError(f"Discord API request failed ({response.status_code}{suffix})")
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("Discord API returned an unexpected response")
        return payload

    def _require_database_url(self) -> str:
        if not self._database_url:
            raise RuntimeError(f"{CENTAUR_POSTGRES_DSN_ENV} is required")
        return self._database_url

    async def _connect(self) -> asyncpg.Connection:
        return await asyncpg.connect(
            _database_url_with_name(self._require_database_url(), _postgres_database_name()),
            command_timeout=30,
            server_settings={
                "application_name": "centaur-investigator",
                "default_transaction_read_only": "on",
            },
        )

    async def _safe_fetch(
        self,
        conn: asyncpg.Connection,
        label: str,
        query: str,
        *args: Any,
    ) -> dict[str, Any]:
        try:
            rows = await conn.fetch(query, *args)
            return {
                "status": "ok",
                "count": len(rows),
                "rows": [_record_to_dict(row) for row in rows],
            }
        except Exception as exc:
            return {"status": "unavailable", "label": label, "error": str(exc), "rows": []}

    async def _safe_fetchrow(
        self,
        conn: asyncpg.Connection,
        label: str,
        query: str,
        *args: Any,
    ) -> dict[str, Any]:
        try:
            row = await conn.fetchrow(query, *args)
            return {"status": "ok", "row": _record_to_dict(row) if row else None}
        except Exception as exc:
            return {"status": "unavailable", "label": label, "error": str(exc), "row": None}

    def parse_thread_reference(self, reference: str) -> dict[str, Any]:
        """Parse a Slack/Discord permalink or Centaur thread key."""
        discord = parse_discord_reference(reference)
        if discord.get("status") == "ok":
            return discord
        return parse_slack_reference(reference)

    async def _session_state_async(
        self,
        thread_key: str,
        *,
        limit: int,
        include_observability: bool,
        window_hours: int,
        logs_limit: int,
    ) -> dict[str, Any]:
        if not thread_key.strip() or not _KEY_SOURCE_RE.match(thread_key):
            return {"status": "error", "error": "thread_key must be namespaced"}

        conn = await self._connect()
        try:
            result = await self._collect_state(
                conn,
                parsed={
                    "kind": "thread_key",
                    "thread_key": thread_key.strip(),
                    "thread_key_candidates": [thread_key.strip()],
                    "thread_key_like": None,
                    "channel_key_like": None,
                    "channel_id": None,
                    "thread_ts": None,
                },
                limit=limit,
            )
        finally:
            await conn.close()

        if include_observability:
            result["observability"] = self._observability(
                thread_keys=result.get("thread_keys") or [thread_key.strip()],
                execution_ids=result.get("execution_ids") or [],
                window_hours=window_hours,
                logs_limit=logs_limit,
            )
        return result

    def session_state(
        self,
        thread_key: str,
        limit: int = DEFAULT_LIMIT,
        include_observability: bool = True,
        window_hours: int = DEFAULT_WINDOW_HOURS,
        logs_limit: int = 100,
    ) -> dict[str, Any]:
        """Inspect source-of-truth state for a known thread_key."""
        try:
            return asyncio.run(
                self._session_state_async(
                    thread_key,
                    limit=_clamp(limit, minimum=1, maximum=MAX_LIMIT),
                    include_observability=include_observability,
                    window_hours=_clamp(window_hours, minimum=1, maximum=MAX_WINDOW_HOURS),
                    logs_limit=_clamp(logs_limit, minimum=1, maximum=MAX_LOG_LIMIT),
                )
            )
        except Exception as exc:
            return {"status": "error", "error": str(exc)}

    async def _collect_state(
        self,
        conn: asyncpg.Connection,
        *,
        parsed: dict[str, Any],
        limit: int,
    ) -> dict[str, Any]:
        candidates = parsed.get("thread_key_candidates") or [parsed.get("thread_key")]
        candidates = [str(value) for value in candidates if value]
        thread_key_like = parsed.get("thread_key_like")
        channel_key_like = parsed.get("channel_key_like")
        channel_id = parsed.get("channel_id")
        thread_ts = parsed.get("thread_ts")
        thread_dt = _slack_ts_to_datetime(thread_ts)
        if thread_dt is None and parsed.get("event_datetime"):
            try:
                thread_dt = datetime.fromisoformat(str(parsed["event_datetime"]))
            except ValueError:
                thread_dt = None

        connection = await self._safe_fetchrow(
            conn,
            "connection_role",
            """
            SELECT
                session_user,
                current_user,
                current_setting('role', true) AS active_role
            """,
        )
        sessions = await self._safe_fetch(
            conn,
            "sessions",
            """
            SELECT
                thread_key,
                sandbox_id,
                harness_type,
                harness_thread_id,
                persona_id,
                status,
                metadata ->> 'source' AS source,
                metadata ->> 'platform' AS platform,
                metadata ->> 'thread_id' AS external_thread_id,
                created_at,
                updated_at
            FROM sessions
            WHERE thread_key = ANY($1::text[])
               OR ($2::text IS NOT NULL AND thread_key LIKE $2)
            ORDER BY updated_at DESC NULLS LAST, created_at DESC
            LIMIT $3
            """,
            candidates,
            thread_key_like,
            limit,
        )
        matched_thread_keys = _dedupe(
            [str(row.get("thread_key")) for row in sessions["rows"] if row.get("thread_key")]
            + candidates
        )

        executions = await self._safe_fetch(
            conn,
            "session_executions",
            """
            SELECT
                execution_id,
                thread_key,
                status,
                metadata ->> 'model' AS model,
                metadata ->> 'harness_run_id' AS harness_run_id,
                metadata ->> 'base_image_ref' AS base_image_ref,
                metadata ->> 'base_image_hash' AS base_image_hash,
                metadata ->> 'overlay_hash' AS overlay_hash,
                metadata ->> 'source' AS source,
                metadata ->> 'platform' AS platform,
                metadata ->> 'action' AS action,
                CASE
                    WHEN metadata ->> 'idle_timeout_ms' ~ '^[0-9]+$'
                    THEN (metadata ->> 'idle_timeout_ms')::bigint
                END AS idle_timeout_ms,
                CASE
                    WHEN metadata ->> 'max_duration_ms' ~ '^[0-9]+$'
                    THEN (metadata ->> 'max_duration_ms')::bigint
                END AS max_duration_ms,
                created_at,
                updated_at,
                started_at,
                completed_at,
                extract(epoch FROM completed_at - started_at) AS duration_seconds
            FROM session_executions
            WHERE thread_key = ANY($1::text[])
               OR ($2::text IS NOT NULL AND thread_key LIKE $2)
            ORDER BY created_at DESC
            LIMIT $3
            """,
            matched_thread_keys,
            thread_key_like,
            limit,
        )
        execution_ids = _dedupe(
            [str(row.get("execution_id")) for row in executions["rows"] if row.get("execution_id")]
        )

        messages = await self._safe_fetch(
            conn,
            "session_messages",
            """
            SELECT
                message_id,
                thread_key,
                role,
                CASE
                    WHEN jsonb_typeof(parts) = 'array' THEN jsonb_array_length(parts)
                    ELSE 0
                END AS part_count,
                coalesce(
                    (
                        SELECT jsonb_agg(distinct coalesce(part_values.part ->> 'type', 'unknown'))
                        FROM jsonb_array_elements(
                            CASE
                                WHEN jsonb_typeof(parts) = 'array' THEN parts
                                ELSE '[]'::jsonb
                            END
                        ) AS part_values(part)
                    ),
                    '[]'::jsonb
                ) AS part_types,
                metadata ->> 'source' AS source,
                metadata ->> 'platform' AS platform,
                metadata ->> 'action' AS action,
                metadata ->> 'user_id' AS user_id,
                metadata ->> 'user_name' AS user_name,
                created_at
            FROM session_messages
            WHERE thread_key = ANY($1::text[])
               OR ($2::text IS NOT NULL AND thread_key LIKE $2)
            ORDER BY created_at ASC, message_id ASC
            LIMIT $3
            """,
            matched_thread_keys,
            thread_key_like,
            limit,
        )
        events = await self._safe_fetch(
            conn,
            "session_events",
            """
            SELECT
                event_id,
                thread_key,
                execution_id,
                event_type,
                payload ->> 'type' AS payload_type,
                payload ->> 'subtype' AS payload_subtype,
                payload ->> 'status' AS status,
                payload ->> 'terminal_reason' AS terminal_reason,
                payload ->> 'turn_id' AS turn_id,
                payload ? 'error' AS has_error,
                CASE
                    WHEN payload ? 'error' THEN octet_length(payload ->> 'error')
                END AS error_length,
                coalesce(
                    CASE
                    WHEN jsonb_typeof(payload) = 'object' THEN (
                        SELECT jsonb_agg(payload_keys.key)
                        FROM jsonb_object_keys(payload) AS payload_keys(key)
                    )
                    END,
                    '[]'::jsonb
                ) AS payload_keys,
                created_at
            FROM session_events
            WHERE event_type IS DISTINCT FROM 'session.output.line'
              AND (
                   thread_key = ANY($1::text[])
                OR ($2::text IS NOT NULL AND thread_key LIKE $2)
                OR (execution_id = ANY($3::text[]))
              )
            ORDER BY event_id ASC
            LIMIT $4
            """,
            matched_thread_keys,
            thread_key_like,
            execution_ids,
            limit * 4,
        )
        legacy_runtime = await self._safe_fetch(
            conn,
            "agent_runtime_assignments",
            """
            SELECT
                thread_key,
                assignment_generation,
                runtime_id,
                harness,
                engine,
                persona_id,
                prompt_ref,
                effective_agents_md_sha256,
                state,
                created_at,
                updated_at,
                released_at
            FROM agent_runtime_assignments
            WHERE thread_key = ANY($1::text[])
               OR ($2::text IS NOT NULL AND thread_key LIKE $2)
            ORDER BY updated_at DESC NULLS LAST
            LIMIT $3
            """,
            matched_thread_keys,
            thread_key_like,
            limit,
        )
        legacy_executions = await self._safe_fetch(
            conn,
            "agent_execution_requests",
            """
            SELECT
                execution_id,
                thread_key,
                assignment_generation,
                execute_id,
                durable_turn_id,
                status,
                created_at,
                claimed_at,
                started_at,
                last_progress_at,
                silence_deadline_at,
                hard_deadline_at,
                stream_break_count,
                last_stream_break_at,
                completed_at,
                terminal_reason,
                worker_id IS NOT NULL AS claimed,
                updated_at
            FROM agent_execution_requests
            WHERE thread_key = ANY($1::text[])
               OR ($2::text IS NOT NULL AND thread_key LIKE $2)
            ORDER BY created_at DESC
            LIMIT $3
            """,
            matched_thread_keys,
            thread_key_like,
            limit,
        )
        sandbox_sessions = await self._safe_fetch(
            conn,
            "sandbox_sessions",
            """
            SELECT
                thread_key,
                sandbox_id,
                harness,
                engine,
                state,
                last_delivered_id,
                agent_thread_id,
                inflight_turn_id,
                inflight_started_at,
                inflight_attempts,
                last_result_at,
                trace_id,
                started_at,
                updated_at,
                wire_connected_at,
                wire_last_seen_at
            FROM sandbox_sessions
            WHERE thread_key = ANY($1::text[])
               OR ($2::text IS NOT NULL AND thread_key LIKE $2)
            ORDER BY updated_at DESC NULLS LAST
            LIMIT $3
            """,
            matched_thread_keys,
            thread_key_like,
            limit,
        )
        traces = await self._safe_fetch(
            conn,
            "thread_traces",
            """
            SELECT
                thread_key,
                trace_id,
                root_span_id,
                created_at,
                updated_at
            FROM thread_traces
            WHERE thread_key = ANY($1::text[])
               OR ($2::text IS NOT NULL AND thread_key LIKE $2)
            ORDER BY updated_at DESC NULLS LAST
            LIMIT $3
            """,
            matched_thread_keys,
            thread_key_like,
            limit,
        )

        nearby_sessions = {"status": "ok", "count": 0, "rows": []}
        if channel_key_like and thread_dt is not None:
            nearby_sessions = await self._safe_fetch(
                conn,
                "nearby_sessions",
                """
                SELECT
                    thread_key,
                    sandbox_id,
                    harness_type,
                    harness_thread_id,
                    persona_id,
                    status,
                    metadata ->> 'source' AS source,
                    metadata ->> 'platform' AS platform,
                    metadata ->> 'thread_id' AS external_thread_id,
                    created_at,
                    updated_at
                FROM sessions
                WHERE thread_key LIKE $1
                  AND created_at BETWEEN
                      ($2::timestamptz - ($3::int * interval '1 hour'))
                      AND ($2::timestamptz + ($3::int * interval '1 hour'))
                ORDER BY abs(extract(epoch FROM created_at - $2::timestamptz)) ASC
                LIMIT $4
                """,
                channel_key_like,
                thread_dt,
                24,
                limit,
            )

        slack: dict[str, Any] = {}
        if parsed.get("source") == "slack" and channel_id:
            slack["channel"] = await self._safe_fetchrow(
                conn,
                "slack_sync_channel",
                """
                SELECT
                    channel_id,
                    channel_name,
                    is_archived,
                    is_syncable,
                    member_count,
                    first_seen_at,
                    last_seen_at,
                    updated_at
                FROM slack_sync_channels
                WHERE channel_id = $1
                """,
                channel_id,
            )
            slack["checkpoint"] = await self._safe_fetchrow(
                conn,
                "slack_sync_checkpoint",
                """
                SELECT
                    channel_id,
                    watermark_ts,
                    last_run_id,
                    last_success_at,
                    last_error <> '' AS has_error,
                    created_at,
                    updated_at
                FROM slack_sync_checkpoints
                WHERE channel_id = $1
                """,
                channel_id,
            )
            slack["messages"] = await self._safe_fetch(
                conn,
                "slack_sync_messages",
                """
                SELECT
                    channel_id,
                    message_ts,
                    occurred_at,
                    thread_ts,
                    parent_message_ts,
                    is_thread_root,
                    user_id,
                    bot_id <> '' AS has_bot_id,
                    message_type,
                    message_subtype,
                    permalink,
                    reply_count,
                    latest_reply_ts,
                    thread_refreshed_at,
                    source_run_id,
                    first_seen_at,
                    last_seen_at,
                    updated_at
                FROM slack_sync_messages
                WHERE channel_id = $1
                  AND (
                      $2::text IS NULL
                      OR message_ts = $2
                      OR thread_ts = $2
                      OR parent_message_ts = $2
                  )
                ORDER BY occurred_at ASC NULLS LAST, message_ts ASC
                LIMIT $3
                """,
                channel_id,
                thread_ts,
                limit * 4,
            )
            message_ts_values = [
                str(row["message_ts"]) for row in slack["messages"]["rows"] if row.get("message_ts")
            ]
            slack["message_attachments"] = await self._safe_fetch(
                conn,
                "slack_sync_message_attachments",
                """
                SELECT
                    channel_id,
                    message_ts,
                    slack_file_id,
                    name,
                    title,
                    mimetype,
                    filetype,
                    size_bytes,
                    permalink,
                    download_status,
                    download_error <> '' AS has_download_error,
                    content_sha256 IS NOT NULL AS has_content_hash,
                    source_run_id,
                    first_seen_at,
                    last_seen_at,
                    updated_at
                FROM slack_sync_message_attachments
                WHERE channel_id = $1
                  AND message_ts = ANY($2::text[])
                ORDER BY updated_at DESC, slack_file_id ASC
                LIMIT $3
                """,
                channel_id,
                message_ts_values,
                limit * 2,
            )
            slack["backfill_jobs"] = await self._safe_fetch(
                conn,
                "slack_sync_backfill_jobs",
                """
                SELECT
                    job_id,
                    job_key,
                    job_type,
                    channel_id,
                    payload_json ->> 'thread_ts' AS thread_ts,
                    status,
                    priority,
                    attempt_count,
                    last_run_id,
                    last_enqueued_at,
                    last_started_at,
                    last_completed_at,
                    last_error <> '' AS has_error,
                    created_at,
                    updated_at
                FROM slack_sync_backfill_jobs
                WHERE channel_id = $1
                  AND ($2::text IS NULL OR payload_json ->> 'thread_ts' = $2)
                ORDER BY updated_at DESC
                LIMIT $3
                """,
                channel_id,
                thread_ts,
                limit,
            )
            slack["recent_sync_runs"] = await self._safe_fetch(
                conn,
                "slack_sync_runs",
                """
                SELECT
                    run_id,
                    workflow_run_id,
                    mode,
                    status,
                    channels_requested,
                    channels_synced,
                    channels_skipped,
                    channels_failed,
                    messages_fetched,
                    messages_upserted,
                    threads_fetched,
                    replies_fetched,
                    replies_upserted,
                    started_at,
                    finished_at,
                    error_text <> '' AS has_error,
                    metadata ->> 'source' AS source
                FROM slack_sync_runs
                WHERE channels_requested ? $1
                   OR channels_synced ? $1
                   OR channels_failed ? $1
                   OR channels_skipped ? $1
                ORDER BY started_at DESC
                LIMIT $2
                """,
                channel_id,
                min(limit, 20),
            )

        result = {
            "status": "ok",
            "parsed": parsed,
            "thread_keys": matched_thread_keys,
            "execution_ids": execution_ids,
            "analysis": self._summarize(
                parsed=parsed,
                sessions=sessions,
                executions=executions,
                messages=messages,
                events=events,
                legacy_runtime=legacy_runtime,
                legacy_executions=legacy_executions,
                sandbox_sessions=sandbox_sessions,
                slack=slack,
            ),
            "postgres": {
                "status": "ok",
                "role": _connection_role(connection),
                "connection": connection,
                "sessions": sessions,
                "nearby_sessions": nearby_sessions,
                "session_executions": executions,
                "session_messages": messages,
                "session_events": events,
                "legacy_agent_runtime_assignments": legacy_runtime,
                "legacy_agent_execution_requests": legacy_executions,
                "legacy_sandbox_sessions": sandbox_sessions,
                "thread_traces": traces,
                "slack": slack,
            },
        }
        return result

    @staticmethod
    def _summarize(
        *,
        parsed: dict[str, Any],
        sessions: dict[str, Any],
        executions: dict[str, Any],
        messages: dict[str, Any],
        events: dict[str, Any],
        legacy_runtime: dict[str, Any],
        legacy_executions: dict[str, Any],
        sandbox_sessions: dict[str, Any],
        slack: dict[str, Any],
    ) -> dict[str, Any]:
        findings: list[str] = []
        warnings: list[str] = []

        if sessions.get("rows"):
            statuses = sorted(
                {str(row.get("status")) for row in sessions["rows"] if row.get("status")}
            )
            findings.append(f"Found {len(sessions['rows'])} session row(s): {', '.join(statuses)}.")
        else:
            warnings.append("No session row matched the parsed thread key candidates.")

        if executions.get("rows"):
            terminal = [row for row in executions["rows"] if row.get("completed_at")]
            active = [
                row for row in executions["rows"] if row.get("status") in {"queued", "running"}
            ]
            findings.append(
                f"Found {len(executions['rows'])} execution row(s), "
                f"{len(active)} active and {len(terminal)} completed."
            )
            latest = executions["rows"][0]
            findings.append(
                "Latest execution "
                f"{latest.get('execution_id')} is {latest.get('status')}"
                + (
                    f" after {latest.get('duration_seconds')}s."
                    if latest.get("duration_seconds")
                    else "."
                )
            )
        else:
            warnings.append("No session execution matched this thread.")

        event_errors = [row for row in events.get("rows", []) if row.get("has_error")]
        if events.get("rows"):
            findings.append(f"Found {len(events['rows'])} sanitized session event row(s).")
        if event_errors:
            warnings.append(f"{len(event_errors)} session event row(s) indicate an error payload.")

        if messages.get("rows"):
            roles = sorted({str(row.get("role")) for row in messages["rows"] if row.get("role")})
            findings.append(
                f"Found {len(messages['rows'])} sanitized message row(s): {', '.join(roles)}."
            )

        if (
            legacy_runtime.get("rows")
            or legacy_executions.get("rows")
            or sandbox_sessions.get("rows")
        ):
            findings.append(
                "Runtime state is present: "
                f"{len(legacy_runtime.get('rows', []))} assignment(s), "
                f"{len(legacy_executions.get('rows', []))} execution request(s), "
                f"{len(sandbox_sessions.get('rows', []))} sandbox session(s)."
            )

        slack_messages = slack.get("messages", {}).get("rows", [])
        if parsed.get("channel_id") and not slack:
            warnings.append("Slack sync tables were not queried.")
        elif parsed.get("channel_id") and slack_messages:
            roots = [row for row in slack_messages if row.get("is_thread_root")]
            findings.append(
                f"Slack sync has {len(slack_messages)} message row(s) for the thread, "
                f"including {len(roots)} root row(s)."
            )
        elif parsed.get("channel_id"):
            warnings.append("Slack sync has no sanitized message row for this thread.")

        backfills = slack.get("backfill_jobs", {}).get("rows", [])
        failed_backfills = [row for row in backfills if row.get("has_error")]
        active_backfills = [
            row for row in backfills if row.get("status") in {"pending", "running", "claimed"}
        ]
        if active_backfills:
            findings.append(f"{len(active_backfills)} Slack backfill job(s) are still active.")
        if failed_backfills:
            warnings.append(f"{len(failed_backfills)} Slack backfill job(s) have errors.")

        channel = slack.get("channel", {}).get("row") if slack else None
        if channel:
            findings.append(
                "Slack channel "
                f"{channel.get('channel_id')} #{channel.get('channel_name')} "
                f"syncable={channel.get('is_syncable')} archived={channel.get('is_archived')}."
            )

        return {
            "summary": (
                " ".join(findings)
                if findings
                else "No matching Centaur source-of-truth state found."
            ),
            "findings": findings,
            "warnings": warnings,
            "primary_source": "postgres_readonly_tables",
        }

    @staticmethod
    def _discord_timeline(
        *,
        discord: dict[str, Any],
        postgres: dict[str, Any],
        observability: dict[str, Any] | None,
    ) -> list[dict[str, Any]]:
        source_message = discord.get("source_message") or {}
        target_message = discord.get("message") or {}
        origin = _parse_datetime(source_message.get("timestamp"))
        rows: list[dict[str, Any]] = []

        def add(stage: str, at: Any, source: str, details: dict[str, Any] | None = None) -> None:
            timestamp = _parse_datetime(at)
            if timestamp is None:
                return
            offset_ms = None
            if origin is not None:
                offset_ms = round((timestamp - origin).total_seconds() * 1000)
            rows.append(
                {
                    "at": timestamp.isoformat(),
                    "offset_ms": offset_ms,
                    "stage": stage,
                    "source": source,
                    "details": details or {},
                }
            )

        add(
            "discord.source_message_created",
            source_message.get("timestamp"),
            "discord_api",
            {"message_id": source_message.get("id")},
        )
        for session in (postgres.get("sessions") or {}).get("rows", []):
            add(
                "session.created",
                session.get("created_at"),
                "postgres",
                {"thread_key": session.get("thread_key"), "status": session.get("status")},
            )
        for execution in (postgres.get("session_executions") or {}).get("rows", []):
            details = {
                "execution_id": execution.get("execution_id"),
                "status": execution.get("status"),
            }
            add("execution.created", execution.get("created_at"), "postgres", details)
            add("execution.started", execution.get("started_at"), "postgres", details)
            add("execution.completed", execution.get("completed_at"), "postgres", details)
        for event in (postgres.get("session_events") or {}).get("rows", []):
            add(
                str(event.get("event_type") or "session.event"),
                event.get("created_at"),
                "postgres",
                {
                    "execution_id": event.get("execution_id"),
                    "status": event.get("status"),
                    "has_error": bool(event.get("has_error")),
                },
            )
        safe_logs = ((observability or {}).get("vlogs") or {}).get("discord_timeline") or []
        for event in safe_logs:
            at = event.get("_time") or event.get("timestamp")
            details = {
                key: value
                for key, value in event.items()
                if key not in {"_time", "timestamp", "event", "service"}
            }
            add(
                str(event.get("event") or "log.event"),
                at,
                str(event.get("service") or "observability"),
                details,
            )
        if target_message.get("id") != source_message.get("id"):
            add(
                "discord.response_message_created",
                target_message.get("timestamp"),
                "discord_api",
                {"message_id": target_message.get("id")},
            )
        rows.sort(key=lambda row: (str(row.get("at") or ""), str(row.get("stage") or "")))
        return rows

    @staticmethod
    def _discord_findings(
        *,
        discord: dict[str, Any],
        postgres: dict[str, Any],
        timeline: list[dict[str, Any]],
    ) -> dict[str, Any]:
        message = discord.get("message") or {}
        source_message = discord.get("source_message") or {}
        content = str(message.get("content") or "")
        findings: list[str] = []
        warnings: list[str] = []

        if message.get("id") != source_message.get("id"):
            findings.append(
                f"The permalink targets a bot response to message {source_message.get('id')}."
            )
        source_time = _parse_datetime(source_message.get("timestamp"))
        response_time = _parse_datetime(message.get("timestamp"))
        response_latency_ms = None
        if source_time is not None and response_time is not None and message != source_message:
            response_latency_ms = round((response_time - source_time).total_seconds() * 1000)
            findings.append(f"Discord response latency was {response_latency_ms}ms.")

        event_rows = (postgres.get("session_events") or {}).get("rows", [])
        execution_rows = (postgres.get("session_executions") or {}).get("rows", [])
        execution_candidates: list[tuple[datetime, dict[str, Any]]] = []
        for execution in execution_rows:
            created_at = _parse_datetime(execution.get("created_at"))
            if created_at is None or source_time is None or created_at < source_time:
                continue
            if response_time is not None and created_at > response_time:
                continue
            execution_candidates.append((created_at, execution))
        selected_execution = (
            min(execution_candidates, key=lambda item: item[0])[1] if execution_candidates else None
        )
        selected_execution_id = (
            str(selected_execution.get("execution_id"))
            if selected_execution and selected_execution.get("execution_id")
            else None
        )
        execution_start = (
            _parse_datetime(selected_execution.get("created_at"))
            if selected_execution
            else source_time
        )
        execution_end = (
            _parse_datetime(selected_execution.get("completed_at"))
            if selected_execution
            else response_time
        ) or response_time

        def belongs_to_selected_execution(row: dict[str, Any], time_key: str) -> bool:
            row_execution_id = row.get("execution_id")
            if row_execution_id:
                return bool(
                    selected_execution_id and str(row_execution_id) == selected_execution_id
                )
            at = _parse_datetime(row.get(time_key))
            return bool(
                at
                and execution_start
                and at >= execution_start
                and (execution_end is None or at <= execution_end)
            )

        warm_pool_hit = any(
            row.get("event_type") == "session.warm_sandbox_claimed"
            and belongs_to_selected_execution(row, "created_at")
            for row in event_rows
        ) or any(
            row.get("stage") == "sandbox_ensure_warm_claimed"
            and belongs_to_selected_execution({**row, **(row.get("details") or {})}, "at")
            for row in timeline
        )
        if warm_pool_hit:
            findings.append("The execution claimed a warm sandbox.")

        sandbox_resumed = any(
            row.get("event_type") == "session.sandbox_resumed"
            and belongs_to_selected_execution(row, "created_at")
            for row in event_rows
        ) or any(
            row.get("stage") in {"session.sandbox_resumed", "sandbox_resumed"}
            and belongs_to_selected_execution({**row, **(row.get("details") or {})}, "at")
            for row in timeline
        )
        if sandbox_resumed:
            findings.append("The execution resumed its assigned sandbox.")

        diagnosis = None
        if "CENTAUR_THREAD_KEY" in content and "missing" in content.lower():
            diagnosis = "missing_thread_context"
            warnings.append(
                "The response reports missing execution-bound thread context before the "
                "application capability could run."
            )

        return {
            "summary": " ".join(findings + warnings) or "Discord message resolved successfully.",
            "findings": findings,
            "warnings": warnings,
            "diagnosis": diagnosis,
            "response_latency_ms": response_latency_ms,
            "sandbox_resumed": sandbox_resumed,
            "warm_pool_hit": warm_pool_hit,
            "primary_source": "discord_api_and_postgres_readonly_tables",
        }

    async def _investigate_discord_message_async(
        self,
        reference: str,
        *,
        limit: int,
        include_observability: bool,
        window_hours: int,
        logs_limit: int,
        include_content: bool,
    ) -> dict[str, Any]:
        parsed = parse_discord_reference(reference)
        if parsed.get("status") != "ok":
            return parsed
        if not parsed.get("message_id"):
            return {
                "status": "error",
                "error": "Discord message investigation requires a permalink with a message id",
            }

        channel_id = str(parsed["channel_id"])
        target = self._discord_get(f"/channels/{channel_id}/messages/{parsed['message_id']}")
        source = target
        referenced = target.get("referenced_message")
        reference_id = (
            (target.get("message_reference") or {}).get("message_id")
            if isinstance(target.get("message_reference"), dict)
            else None
        )
        if _discord_author(target).get("bot") and reference_id:
            if isinstance(referenced, dict) and str(referenced.get("id") or "") == str(
                reference_id
            ):
                source = referenced
            else:
                source = self._discord_get(f"/channels/{channel_id}/messages/{reference_id}")

        channel = self._discord_get(f"/channels/{channel_id}")
        parent_channel_id = (
            str(channel.get("parent_id"))
            if channel.get("type") in {10, 11, 12} and channel.get("parent_id")
            else None
        )
        source_message_id = str(source.get("id") or parsed["message_id"])
        parsed.update(
            {
                "source_message_id": source_message_id,
                "target_message_id": str(target.get("id") or parsed["message_id"]),
                "parent_channel_id": parent_channel_id,
                "event_datetime": source.get("timestamp") or parsed.get("event_datetime"),
                "thread_key_candidates": _discord_thread_key_candidates(
                    guild_id=str(parsed["guild_id"]),
                    channel_id=channel_id,
                    message_id=source_message_id,
                    parent_channel_id=parent_channel_id,
                ),
                "channel_key_like": (
                    f"discord:{parsed['guild_id']}:{parent_channel_id or channel_id}%"
                ),
            }
        )

        message_ids = _dedupe(
            [
                str(value)
                for value in [parsed.get("message_id"), source_message_id, reference_id]
                if value
            ]
        )
        conn = await self._connect()
        try:
            matched = await self._safe_fetch(
                conn,
                "discord_message_sessions",
                """
                SELECT DISTINCT thread_key
                FROM session_messages
                WHERE metadata ->> 'platform' = 'discord'
                  AND metadata ->> 'message_id' = ANY($1::text[])
                ORDER BY thread_key
                LIMIT $2
                """,
                message_ids,
                limit,
            )
            matched_keys = [
                str(row.get("thread_key"))
                for row in matched.get("rows", [])
                if row.get("thread_key")
            ]
            parsed["thread_key_candidates"] = _dedupe(
                matched_keys + list(parsed["thread_key_candidates"])
            )
            if matched_keys:
                parsed["thread_key"] = matched_keys[0]

            # Inline-reply answers deliberately reference the chain root in
            # Discord, not the user message that triggered each follow-up
            # execution. Correlate the target response to the latest execution
            # that began before it, then select the latest user message that
            # preceded that execution. This keeps response latency and source
            # content tied to the actual turn without changing the root-based
            # Centaur thread key.
            target_time = _parse_datetime(target.get("timestamp"))
            if matched_keys and target_time is not None and target is not source:
                trigger = await self._safe_fetchrow(
                    conn,
                    "discord_trigger_message",
                    """
                    WITH target_execution AS (
                        SELECT thread_key, created_at
                        FROM session_executions
                        WHERE thread_key = ANY($1::text[])
                          AND created_at <= $2::timestamptz
                        ORDER BY created_at DESC
                        LIMIT 1
                    )
                    SELECT
                        message.metadata ->> 'message_id' AS platform_message_id,
                        message.metadata ->> 'timestamp' AS message_timestamp
                    FROM session_messages AS message
                    JOIN target_execution AS execution
                      ON execution.thread_key = message.thread_key
                    WHERE message.role = 'user'
                      AND message.created_at <= execution.created_at
                      AND message.metadata ->> 'platform' = 'discord'
                      AND message.metadata ->> 'message_id' IS NOT NULL
                    ORDER BY message.created_at DESC
                    LIMIT 1
                    """,
                    matched_keys,
                    target_time,
                )
                trigger_row = trigger.get("row") or {}
                trigger_message_id = str(trigger_row.get("platform_message_id") or "")
                if trigger_message_id and trigger_message_id != source_message_id:
                    try:
                        correlated_source = self._discord_get(
                            f"/channels/{channel_id}/messages/{trigger_message_id}"
                        )
                    except Exception:
                        correlated_source = None
                    if correlated_source:
                        parsed["reply_root_message_id"] = source_message_id
                        source = correlated_source
                        source_message_id = trigger_message_id
                        parsed["source_message_id"] = source_message_id
                        parsed["event_datetime"] = (
                            source.get("timestamp")
                            or trigger_row.get("message_timestamp")
                            or parsed.get("event_datetime")
                        )
                        message_ids = _dedupe(message_ids + [source_message_id])
            result = await self._collect_state(conn, parsed=parsed, limit=limit)
        finally:
            await conn.close()

        discord = {
            "status": "ok",
            "message": _sanitize_discord_message(target, include_content=include_content),
            "source_message": _sanitize_discord_message(source, include_content=include_content),
            "channel": {
                "id": str(channel.get("id") or channel_id),
                "name": channel.get("name"),
                "type": channel.get("type"),
                "parent_id": parent_channel_id,
            },
        }
        result["discord"] = discord
        observability = None
        if include_observability:
            observability = self._observability(
                thread_keys=result.get("thread_keys") or parsed["thread_key_candidates"],
                execution_ids=result.get("execution_ids") or [],
                window_hours=window_hours,
                logs_limit=logs_limit,
                discord_message_ids=message_ids,
            )
            result["observability"] = observability
        timeline = self._discord_timeline(
            discord=discord,
            postgres=result.get("postgres") or {},
            observability=observability,
        )
        result["timeline"] = timeline
        result["analysis"] = self._discord_findings(
            discord=discord,
            postgres=result.get("postgres") or {},
            timeline=timeline,
        )
        return result

    def investigate_discord_message(
        self,
        reference: str,
        limit: int = DEFAULT_LIMIT,
        include_observability: bool = True,
        window_hours: int = DEFAULT_WINDOW_HOURS,
        logs_limit: int = 100,
        include_content: bool = True,
    ) -> dict[str, Any]:
        """Investigate a Discord permalink using bot-scoped reads and readonly state."""
        try:
            return asyncio.run(
                self._investigate_discord_message_async(
                    reference,
                    limit=_clamp(limit, minimum=1, maximum=MAX_LIMIT),
                    include_observability=include_observability,
                    window_hours=_clamp(window_hours, minimum=1, maximum=MAX_WINDOW_HOURS),
                    logs_limit=_clamp(logs_limit, minimum=1, maximum=MAX_LOG_LIMIT),
                    include_content=include_content,
                )
            )
        except Exception as exc:
            return {"status": "error", "error": str(exc)}

    async def _investigate_slack_thread_async(
        self,
        reference: str,
        *,
        limit: int,
        include_observability: bool,
        window_hours: int,
        logs_limit: int,
    ) -> dict[str, Any]:
        parsed = parse_slack_reference(reference)
        if parsed.get("status") != "ok":
            return parsed

        conn = await self._connect()
        try:
            result = await self._collect_state(conn, parsed=parsed, limit=limit)
        finally:
            await conn.close()

        if include_observability:
            result["observability"] = self._observability(
                thread_keys=result.get("thread_keys") or parsed.get("thread_key_candidates") or [],
                execution_ids=result.get("execution_ids") or [],
                window_hours=window_hours,
                logs_limit=logs_limit,
            )
        return result

    def investigate_slack_thread(
        self,
        reference: str,
        limit: int = DEFAULT_LIMIT,
        include_observability: bool = True,
        window_hours: int = DEFAULT_WINDOW_HOURS,
        logs_limit: int = 100,
    ) -> dict[str, Any]:
        """Investigate a Slack thread link with sanitized readonly Postgres metadata."""
        try:
            return asyncio.run(
                self._investigate_slack_thread_async(
                    reference,
                    limit=_clamp(limit, minimum=1, maximum=MAX_LIMIT),
                    include_observability=include_observability,
                    window_hours=_clamp(window_hours, minimum=1, maximum=MAX_WINDOW_HOURS),
                    logs_limit=_clamp(logs_limit, minimum=1, maximum=MAX_LOG_LIMIT),
                )
            )
        except Exception as exc:
            return {"status": "error", "error": str(exc)}

    def investigate(
        self,
        query: str,
        limit: int = DEFAULT_LIMIT,
        include_observability: bool = True,
        window_hours: int = DEFAULT_WINDOW_HOURS,
        logs_limit: int = 100,
    ) -> dict[str, Any]:
        """Investigate text containing a Discord/Slack link or thread_key."""
        discord = parse_discord_reference(query)
        if discord.get("status") == "ok" and discord.get("message_id"):
            return self.investigate_discord_message(
                query,
                limit=limit,
                include_observability=include_observability,
                window_hours=window_hours,
                logs_limit=logs_limit,
            )
        parsed = parse_slack_reference(query)
        if parsed.get("status") == "ok":
            return self.investigate_slack_thread(
                query,
                limit=limit,
                include_observability=include_observability,
                window_hours=window_hours,
                logs_limit=logs_limit,
            )
        direct_key = re.search(r"\b[A-Za-z][A-Za-z0-9_.-]*:[^\s<>|]+\b", query)
        if direct_key:
            return self.session_state(
                direct_key.group(0),
                limit=limit,
                include_observability=include_observability,
                window_hours=window_hours,
                logs_limit=logs_limit,
            )
        return {
            "status": "error",
            "error": "query must contain a Discord/Slack permalink or Centaur thread_key",
        }

    async def _search_sessions_async(
        self,
        *,
        query: str,
        channel_id: str,
        status: str,
        limit: int,
    ) -> dict[str, Any]:
        conn = await self._connect()
        try:
            rows = await conn.fetch(
                """
                SELECT
                    thread_key,
                    sandbox_id,
                    harness_type,
                    harness_thread_id,
                    persona_id,
                    status,
                    metadata ->> 'source' AS source,
                    metadata ->> 'platform' AS platform,
                    metadata ->> 'thread_id' AS external_thread_id,
                    created_at,
                    updated_at
                FROM sessions
                WHERE ($1::text = '' OR thread_key ILIKE '%' || $1 || '%')
                  AND ($2::text = '' OR thread_key LIKE '%:' || $2 || ':%')
                  AND ($3::text = '' OR status = $3)
                ORDER BY updated_at DESC NULLS LAST, created_at DESC
                LIMIT $4
                """,
                query.strip(),
                channel_id.strip(),
                status.strip(),
                limit,
            )
            return {
                "status": "ok",
                "count": len(rows),
                "sessions": [_record_to_dict(row) for row in rows],
            }
        finally:
            await conn.close()

    def search_sessions(
        self,
        query: str = "",
        channel_id: str = "",
        status: str = "",
        limit: int = DEFAULT_LIMIT,
    ) -> dict[str, Any]:
        """Search recent Centaur sessions by thread_key substring, Slack channel, or status."""
        try:
            return asyncio.run(
                self._search_sessions_async(
                    query=query,
                    channel_id=channel_id,
                    status=status,
                    limit=_clamp(limit, minimum=1, maximum=MAX_LIMIT),
                )
            )
        except Exception as exc:
            return {"status": "error", "error": str(exc)}

    def _observability(
        self,
        *,
        thread_keys: list[str],
        execution_ids: list[str],
        window_hours: int,
        logs_limit: int,
        discord_message_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "source": "best_effort_vlogs_vmetrics",
            "window_hours": window_hours,
            "privacy_note": (
                "Only aggregate observability metadata is returned. Raw log rows, "
                "Slack message text, and stored transcript context are never requested."
            ),
            "vlogs": {"status": "skipped"},
            "vmetrics": {"status": "skipped"},
        }

        infra_dir = Path(__file__).resolve().parent.parent
        vlogs_module = _safe_load_module(
            "_centaur_investigator_vlogs_client",
            infra_dir / "vlogs" / "client.py",
        )
        if vlogs_module is not None:
            try:
                vlogs = vlogs_module.VictoriaLogsClient()
                primary_thread = thread_keys[0] if thread_keys else ""
                thread_query = (
                    f"_time:{window_hours}h {_log_field_expr('thread_key', primary_thread)}"
                    if primary_thread
                    else ""
                )
                result["vlogs"] = {
                    "status": "ok",
                    "thread_key": primary_thread,
                    "log_hits": vlogs.hits(thread_query, step="1h") if thread_query else {},
                    "error_hits": (
                        vlogs.hits(f"{thread_query} AND level:error", step="1h")
                        if thread_query
                        else {}
                    ),
                    "event_names": (
                        vlogs.field_values("event", query=thread_query, limit=min(100, logs_limit))
                        if thread_query
                        else []
                    ),
                    "services": (
                        vlogs.field_values("service", query=thread_query, limit=min(50, logs_limit))
                        if thread_query
                        else []
                    ),
                    "tool_usage": (
                        vlogs.tool_usage_by_thread(
                            thread_key=primary_thread,
                            start=f"{window_hours}h",
                            limit=min(100, logs_limit),
                        )
                        if primary_thread
                        else []
                    ),
                    "execution_log_hits": {
                        execution_id: vlogs.hits(
                            (
                                f"_time:{window_hours}h "
                                f"{_log_field_expr('execution_id', execution_id)}"
                            ),
                            step="1h",
                        )
                        for execution_id in execution_ids[:3]
                    },
                }
                if discord_message_ids:
                    selectors = [
                        _log_field_expr(field, value)
                        for value in discord_message_ids[:3]
                        for field in ("message_id", "messageId")
                    ] + [_log_field_expr("execution_id", value) for value in execution_ids[:3]]
                    query = f"_time:{window_hours}h (" + " OR ".join(selectors) + ")"
                    raw_timeline = vlogs.query(query, limit=min(logs_limit, MAX_LOG_LIMIT))
                    result["vlogs"]["discord_timeline"] = sorted(
                        [
                            _safe_discord_log_entry(row)
                            for row in raw_timeline
                            if isinstance(row, dict) and "_note" not in row
                        ],
                        key=lambda row: str(row.get("_time") or row.get("timestamp") or ""),
                    )
            except Exception as exc:
                result["vlogs"] = {"status": "error", "error": str(exc)}

        vmetrics_module = _safe_load_module(
            "_centaur_investigator_vmetrics_client",
            infra_dir / "vmetrics" / "client.py",
        )
        if vmetrics_module is not None:
            try:
                vmetrics = vmetrics_module.VictoriaMetricsClient()
                result["vmetrics"] = {
                    "status": "ok",
                    "ready": vmetrics.ready(),
                    "session_metric_names": vmetrics.metric_names(prefix="session_")[:50],
                    "centaur_metric_names": vmetrics.metric_names(prefix="centaur_")[:50],
                }
            except Exception as exc:
                result["vmetrics"] = {"status": "error", "error": str(exc)}

        return result


def _client() -> CentaurInvestigatorClient:
    return CentaurInvestigatorClient()
