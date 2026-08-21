from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import client as centaur_client
from client import (
    CentaurInvestigatorClient,
    _connection_role,
    _database_url_with_name,
    _postgres_database_name,
    _record_to_dict,
    _safe_discord_log_entry,
    parse_discord_reference,
    parse_slack_reference,
)


class _AsyncpgLikeRecord:
    """Mimic asyncpg.Record, whose iterator yields values rather than keys."""

    def __init__(self, values: dict[str, object]) -> None:
        self._values = values

    def __iter__(self):
        return iter(self._values.values())

    def items(self):
        return self._values.items()


def test_record_to_dict_uses_mapping_items_for_asyncpg_records() -> None:
    row = _AsyncpgLikeRecord({"current_user": "tempo", "execution_id": "exe_1"})

    assert _record_to_dict(row) == {
        "current_user": "tempo",
        "execution_id": "exe_1",
    }


def test_connection_role_falls_back_to_current_user_when_role_is_none() -> None:
    assert (
        _connection_role(
            {"row": {"session_user": "tempo", "current_user": "tempo", "active_role": "none"}}
        )
        == "tempo"
    )


def test_database_url_with_name_appends_database_to_base_dsn() -> None:
    assert (
        _database_url_with_name("postgresql://user:pass@proxy:5432", "ai_v2")
        == "postgresql://user:pass@proxy:5432/ai_v2"
    )
    assert (
        _database_url_with_name("postgresql://user:pass@proxy:5432/other", "ai_v2")
        == "postgresql://user:pass@proxy:5432/other"
    )
    assert (
        _database_url_with_name("postgresql://user:pass@proxy:5432?sslmode=require", "ai_v2")
        == "postgresql://user:pass@proxy:5432/ai_v2?sslmode=require"
    )


def test_postgres_database_name_defaults_to_ai_v2(monkeypatch) -> None:
    monkeypatch.delenv("CENTAUR_INVESTIGATOR_POSTGRES_DATABASE", raising=False)

    assert _postgres_database_name() == "ai_v2"


def test_postgres_database_name_can_be_overridden(monkeypatch) -> None:
    monkeypatch.setenv("CENTAUR_INVESTIGATOR_POSTGRES_DATABASE", "centaur")

    assert _postgres_database_name() == "centaur"


def test_postgres_database_name_uses_default_for_blank_override(monkeypatch) -> None:
    monkeypatch.setenv("CENTAUR_INVESTIGATOR_POSTGRES_DATABASE", " ")

    assert _postgres_database_name() == "ai_v2"


class _FakeConnection:
    def __init__(self) -> None:
        self.execute_calls: list[str] = []
        self.fetch_calls: list[tuple[str, tuple]] = []
        self.fetchrow_calls: list[tuple[str, tuple]] = []
        self.closed = False
        self.now = dt.datetime(2026, 6, 17, 12, 0, tzinfo=dt.UTC)

    async def execute(self, query: str) -> None:
        self.execute_calls.append(query)

    async def fetchrow(self, query: str, *args):
        self.fetchrow_calls.append((query, args))
        if "current_setting('role'" in query:
            return {
                "session_user": "centaur",
                "current_user": "centaur_readonly",
                "active_role": "centaur_readonly",
            }
        if "FROM slack_sync_channels" in query:
            return {
                "channel_id": "C123",
                "channel_name": "eng",
                "is_archived": False,
                "is_syncable": True,
                "member_count": 42,
                "first_seen_at": self.now,
                "last_seen_at": self.now,
                "updated_at": self.now,
            }
        if "FROM slack_sync_checkpoints" in query:
            return {
                "channel_id": "C123",
                "watermark_ts": "1778000000.000000",
                "last_run_id": "run_1",
                "last_success_at": self.now,
                "has_error": False,
                "created_at": self.now,
                "updated_at": self.now,
            }
        return None

    async def fetch(self, query: str, *args):
        self.fetch_calls.append((query, args))
        if "FROM sessions" in query and "BETWEEN" not in query:
            return [
                {
                    "thread_key": "slack:C123:1777910337.403889",
                    "sandbox_id": "asbx_1",
                    "harness_type": "codex",
                    "harness_thread_id": "harness_1",
                    "persona_id": "default",
                    "status": "idle",
                    "source": "slack",
                    "platform": "slack",
                    "external_thread_id": "1777910337.403889",
                    "created_at": self.now,
                    "updated_at": self.now,
                }
            ]
        if "FROM session_executions" in query:
            return [
                {
                    "execution_id": "exe_1",
                    "thread_key": "slack:C123:1777910337.403889",
                    "status": "completed",
                    "model": "gpt-test",
                    "created_at": self.now,
                    "started_at": self.now,
                    "completed_at": self.now,
                    "duration_seconds": 42.0,
                }
            ]
        if "FROM session_messages" in query:
            return [
                {
                    "message_id": "msg_1",
                    "thread_key": "slack:C123:1777910337.403889",
                    "role": "user",
                    "part_count": 1,
                    "part_types": ["text"],
                    "source": "slack",
                    "platform": "slack",
                    "created_at": self.now,
                }
            ]
        if "FROM session_events" in query:
            return [
                {
                    "event_id": 1,
                    "thread_key": "slack:C123:1777910337.403889",
                    "execution_id": "exe_1",
                    "event_type": "session.execution_completed",
                    "payload_type": "result",
                    "payload_keys": ["type", "status"],
                    "has_error": False,
                    "created_at": self.now,
                }
            ]
        if "FROM slack_sync_messages" in query:
            return [
                {
                    "channel_id": "C123",
                    "message_ts": "1777910337.403889",
                    "thread_ts": "1777910337.403889",
                    "is_thread_root": True,
                    "user_id": "U123",
                    "message_type": "message",
                    "reply_count": 2,
                    "updated_at": self.now,
                }
            ]
        if "FROM slack_sync_message_attachments" in query:
            return [
                {
                    "channel_id": "C123",
                    "message_ts": "1777910337.403889",
                    "slack_file_id": "F123",
                    "name": "debug.log",
                    "mimetype": "text/plain",
                    "size_bytes": 100,
                    "download_status": "metadata_only",
                    "has_content_hash": False,
                    "updated_at": self.now,
                }
            ]
        if "FROM slack_sync_backfill_jobs" in query:
            return []
        if "FROM slack_sync_runs" in query:
            return []
        if "FROM sessions" in query and "BETWEEN" in query:
            return []
        if "FROM agent_runtime_assignments" in query:
            raise RuntimeError("relation does not exist")
        if "FROM agent_execution_requests" in query:
            raise RuntimeError("relation does not exist")
        if "FROM sandbox_sessions" in query:
            raise RuntimeError("relation does not exist")
        if "FROM thread_traces" in query:
            raise RuntimeError("relation does not exist")
        return []

    async def close(self) -> None:
        self.closed = True


def test_parse_slack_permalink_prefers_thread_ts_query() -> None:
    result = parse_slack_reference(
        "Investigate https://example.slack.com/archives/C123/p1777910338403889"
        "?thread_ts=1777910337.403889&cid=C123"
    )

    assert result["status"] == "ok"
    assert result["kind"] == "slack_permalink"
    assert result["channel_id"] == "C123"
    assert result["message_ts"] == "1777910338.403889"
    assert result["thread_ts"] == "1777910337.403889"
    assert result["thread_key_candidates"] == [
        "slack:C123:1777910337.403889",
        "chat:C123:1777910337.403889",
    ]
    assert result["thread_key_like"] == "%:C123:1777910337.403889"


def test_parse_slack_thread_key_with_team() -> None:
    result = parse_slack_reference("slack:T0AQQ46PL4C:C0B0XS7BLA3:1780035646.228899")

    assert result["status"] == "ok"
    assert result["team_id"] == "T0AQQ46PL4C"
    assert result["channel_id"] == "C0B0XS7BLA3"
    assert result["thread_key_candidates"][:4] == [
        "slack:T0AQQ46PL4C:C0B0XS7BLA3:1780035646.228899",
        "chat:T0AQQ46PL4C:C0B0XS7BLA3:1780035646.228899",
        "slack:C0B0XS7BLA3:1780035646.228899",
        "chat:C0B0XS7BLA3:1780035646.228899",
    ]


def test_parse_discord_permalink_returns_snowflake_time_and_candidates() -> None:
    result = parse_discord_reference(
        "debug https://discord.com/channels/87003047531651072/"
        "1407795944711524462/1537142385174384830"
    )

    assert result["status"] == "ok"
    assert result["source"] == "discord"
    assert result["guild_id"] == "87003047531651072"
    assert result["channel_id"] == "1407795944711524462"
    assert result["message_id"] == "1537142385174384830"
    assert result["message_datetime"] == "2026-08-12T16:55:03.350000+00:00"
    assert result["thread_key_candidates"] == [
        "discord:87003047531651072:1407795944711524462:reply~1537142385174384830",
        "discord:87003047531651072:1407795944711524462",
    ]


def test_safe_discord_log_entry_redacts_content_errors_args_and_tokens() -> None:
    result = _safe_discord_log_entry(
        {
            "_time": "2026-08-12T16:55:00Z",
            "event": "tool_call_completed",
            "tool_name": "application",
            "duration_ms": 1200,
            "success": "false",
            "content": "private message",
            "text": "private message",
            "error": "secret failure details",
            "tool_args": ["wallet.transfer", {"amount": "100"}],
            "DISCORD_BOT_TOKEN": "never-return-this",
        }
    )

    assert result == {
        "_time": "2026-08-12T16:55:00Z",
        "event": "tool_call_completed",
        "tool_name": "application",
        "duration_ms": 1200,
        "success": "false",
    }


def test_investigation_queries_readonly_tables_without_message_context(monkeypatch) -> None:
    fake = _FakeConnection()

    async def fake_connect(*args, **kwargs):
        return fake

    monkeypatch.setattr(centaur_client.asyncpg, "connect", fake_connect)

    result = CentaurInvestigatorClient("postgresql://example").investigate_slack_thread(
        "https://example.slack.com/archives/C123/p1777910337403889",
        include_observability=False,
    )

    assert result["status"] == "ok"
    assert result["postgres"]["status"] == "ok"
    assert result["postgres"]["role"] == "centaur_readonly"
    assert result["postgres"]["connection"]["row"]["current_user"] == "centaur_readonly"
    assert result["analysis"]["primary_source"] == "postgres_readonly_tables"
    assert fake.execute_calls == []
    assert fake.closed is True

    all_queries = "\n".join(query for query, _args in fake.fetch_calls + fake.fetchrow_calls)
    assert "centaur_readonly_" not in all_queries
    assert "SELECT *" not in all_queries
    assert "FROM sessions" in all_queries
    assert "FROM session_messages" in all_queries
    assert "FROM slack_sync_messages" in all_queries

    assert "raw_payload" not in str(result)
    assert "url_private" not in str(result)
    assert "content_bytes" not in str(result)
    assert "secret user message" not in str(result)


def test_observability_never_requests_raw_log_context(monkeypatch) -> None:
    fake = _FakeConnection()

    async def fake_connect(*args, **kwargs):
        return fake

    class FakeVlogs:
        def hits(self, query: str, step: str | None = None) -> dict:
            return {"query": query, "step": step, "hits": []}

        def field_values(self, field: str, query: str = "*", limit: int = 100) -> list[str]:
            if field == "event":
                return ["message_stored", "execute_completed"]
            return ["api"]

        def tool_usage_by_thread(
            self,
            thread_key: str = "",
            start: str = "24h",
            limit: int = 200,
        ) -> list[dict]:
            return [
                {
                    "_time": "2026-06-17T00:00:00Z",
                    "tool_name": "github",
                    "tool_method": "search",
                    "duration_ms": "42",
                    "success": "true",
                }
            ]

        def thread_trace(self, *args, **kwargs):
            raise AssertionError("raw thread trace should not be requested")

        def errors(self, *args, **kwargs):
            raise AssertionError("raw error logs should not be requested")

        def execution_timeline(self, *args, **kwargs):
            raise AssertionError("raw execution logs should not be requested")

    def fake_load_module(module_name: str, path: Path):
        if "vlogs" in str(path):
            return SimpleNamespace(VictoriaLogsClient=FakeVlogs)
        return None

    monkeypatch.setattr(centaur_client.asyncpg, "connect", fake_connect)
    monkeypatch.setattr(centaur_client, "_safe_load_module", fake_load_module)

    result = CentaurInvestigatorClient("postgresql://example").investigate_slack_thread(
        "https://example.slack.com/archives/C123/p1777910337403889",
        include_observability=True,
    )

    assert result["status"] == "ok"
    assert result["observability"]["vlogs"]["status"] == "ok"
    assert "thread_trace" not in str(result["observability"])
    assert "execution_logs" not in str(result["observability"])
    assert "raw_payload" not in str(result)


class _DiscordFakeConnection(_FakeConnection):
    async def fetch(self, query: str, *args):
        self.fetch_calls.append((query, args))
        thread_key = "discord:87003047531651072:1407795944711524462:reply~1537142298947756043"
        if "SELECT DISTINCT thread_key" in query and "FROM session_messages" in query:
            return [{"thread_key": thread_key}]
        if "FROM sessions" in query and "BETWEEN" not in query:
            return [
                {
                    "thread_key": thread_key,
                    "sandbox_id": "asbx_warm",
                    "harness_type": "codex",
                    "harness_thread_id": "harness_1",
                    "persona_id": None,
                    "status": "idle",
                    "source": "discordbot",
                    "platform": "discord",
                    "external_thread_id": "1537142298947756043",
                    "created_at": dt.datetime(2026, 8, 12, 16, 54, 42, 998000, tzinfo=dt.UTC),
                    "updated_at": dt.datetime(2026, 8, 12, 16, 55, 4, tzinfo=dt.UTC),
                }
            ]
        if "FROM sessions" in query and "BETWEEN" in query:
            return []
        if "FROM session_executions" in query:
            return [
                {
                    "execution_id": "exe_1",
                    "thread_key": thread_key,
                    "status": "completed",
                    "model": "gpt-test",
                    "created_at": dt.datetime(2026, 8, 12, 16, 54, 43, 85000, tzinfo=dt.UTC),
                    "started_at": dt.datetime(2026, 8, 12, 16, 54, 43, 85000, tzinfo=dt.UTC),
                    "completed_at": dt.datetime(2026, 8, 12, 16, 55, 3, 62000, tzinfo=dt.UTC),
                    "duration_seconds": 19.977,
                }
            ]
        if "FROM session_messages" in query:
            return [
                {
                    "message_id": "msg_1",
                    "thread_key": thread_key,
                    "role": "user",
                    "part_count": 1,
                    "part_types": ["text"],
                    "source": "discordbot",
                    "platform": "discord",
                    "user_id": "87002447687467008",
                    "user_name": "slokh",
                    "created_at": dt.datetime(2026, 8, 12, 16, 54, 43, 61000, tzinfo=dt.UTC),
                }
            ]
        if "FROM session_events" in query:
            return [
                {
                    "event_id": 1,
                    "thread_key": thread_key,
                    "execution_id": "exe_1",
                    "event_type": "session.warm_sandbox_claimed",
                    "status": None,
                    "has_error": False,
                    "created_at": dt.datetime(2026, 8, 12, 16, 54, 43, 576000, tzinfo=dt.UTC),
                }
            ]
        if "FROM thread_traces" in query:
            return []
        if any(
            table in query
            for table in (
                "FROM agent_runtime_assignments",
                "FROM agent_execution_requests",
                "FROM sandbox_sessions",
            )
        ):
            return []
        raise AssertionError(f"unexpected query: {query}")


def _discord_http_client(
    trigger_message: dict[str, object] | None = None,
) -> httpx.Client:
    source_message = {
        "id": "1537142298947756043",
        "channel_id": "1407795944711524462",
        "guild_id": "87003047531651072",
        "content": "<@1520495166668931242> balances",
        "timestamp": "2026-08-12T16:54:42.942000+00:00",
        "edited_timestamp": None,
        "author": {
            "id": "87002447687467008",
            "username": "slokh",
            "global_name": "kartik",
        },
        "attachments": [],
        "embeds": [],
        "reactions": [{"emoji": {"name": "✅"}, "count": 1}],
    }
    target_message = {
        "id": "1537142385174384830",
        "channel_id": "1407795944711524462",
        "guild_id": "87003047531651072",
        "content": (
            "I can't retrieve your live balance because this session is missing its "
            "thread context (`CENTAUR_THREAD_KEY`)."
        ),
        "timestamp": "2026-08-12T16:55:03.350000+00:00",
        "edited_timestamp": "2026-08-12T16:55:04.073000+00:00",
        "author": {"id": "1520495166668931242", "username": "ai", "bot": True},
        "message_reference": {"message_id": source_message["id"]},
        "referenced_message": source_message,
        "attachments": [],
        "embeds": [],
        "reactions": [],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bot test-token"
        if request.url.path.endswith("/messages/1537142385174384830"):
            return httpx.Response(200, json=target_message)
        if trigger_message and request.url.path.endswith(f"/messages/{trigger_message['id']}"):
            return httpx.Response(200, json=trigger_message)
        if request.url.path.endswith("/channels/1407795944711524462"):
            return httpx.Response(
                200,
                json={"id": "1407795944711524462", "name": "debug", "type": 0},
            )
        return httpx.Response(404, json={"code": 10008, "message": "Unknown Message"})

    return httpx.Client(
        base_url=centaur_client.DISCORD_API_URL, transport=httpx.MockTransport(handler)
    )


def test_discord_investigation_resolves_bot_reply_and_correlates_execution(monkeypatch) -> None:
    fake = _DiscordFakeConnection()

    async def fake_connect(*args, **kwargs):
        return fake

    monkeypatch.setattr(centaur_client.asyncpg, "connect", fake_connect)
    http_client = _discord_http_client()
    try:
        result = CentaurInvestigatorClient(
            "postgresql://example",
            discord_token="test-token",
            discord_http_client=http_client,
        ).investigate_discord_message(
            "https://discord.com/channels/87003047531651072/"
            "1407795944711524462/1537142385174384830",
            include_observability=False,
        )
    finally:
        http_client.close()

    assert result["status"] == "ok"
    assert result["parsed"]["source_message_id"] == "1537142298947756043"
    assert result["parsed"]["thread_key"].endswith("reply~1537142298947756043")
    assert result["discord"]["message"]["author"]["bot"] is True
    assert result["discord"]["source_message"]["content"].endswith("balances")
    assert result["analysis"]["diagnosis"] == "missing_thread_context"
    assert result["analysis"]["warm_pool_hit"] is True
    assert result["analysis"]["response_latency_ms"] == 20408
    assert any(row["stage"] == "discord.response_message_created" for row in result["timeline"])
    assert fake.closed is True

    serialized = json.dumps(result, default=str)
    assert "test-token" not in serialized
    assert "avatar" not in serialized
    assert "attachment_url" not in serialized


def test_discord_investigation_attributes_inline_reply_to_followup_turn(monkeypatch) -> None:
    followup_id = "1537142360000000000"
    followup = {
        "id": followup_id,
        "channel_id": "1407795944711524462",
        "guild_id": "87003047531651072",
        "content": "in japan",
        "timestamp": "2026-08-12T16:54:58.000000+00:00",
        "edited_timestamp": None,
        "author": {
            "id": "87002447687467008",
            "username": "slokh",
            "global_name": "kartik",
        },
        "attachments": [],
        "embeds": [],
        "reactions": [],
    }

    class ReplyChainConnection(_DiscordFakeConnection):
        async def fetchrow(self, query: str, *args):
            if "WITH target_execution AS" in query:
                return {
                    "platform_message_id": followup_id,
                    "message_timestamp": followup["timestamp"],
                }
            return await super().fetchrow(query, *args)

    fake = ReplyChainConnection()

    async def fake_connect(*args, **kwargs):
        return fake

    monkeypatch.setattr(centaur_client.asyncpg, "connect", fake_connect)
    http_client = _discord_http_client(followup)
    try:
        result = CentaurInvestigatorClient(
            "postgresql://example",
            discord_token="test-token",
            discord_http_client=http_client,
        ).investigate_discord_message(
            "https://discord.com/channels/87003047531651072/"
            "1407795944711524462/1537142385174384830",
            include_observability=False,
        )
    finally:
        http_client.close()

    assert result["status"] == "ok"
    assert result["parsed"]["source_message_id"] == followup_id
    assert result["parsed"]["reply_root_message_id"] == "1537142298947756043"
    assert result["parsed"]["thread_key"].endswith("reply~1537142298947756043")
    assert result["discord"]["source_message"]["content"] == "in japan"
    assert result["analysis"]["response_latency_ms"] == 5350


def test_discord_findings_scope_warm_pool_events_to_current_execution() -> None:
    source_time = dt.datetime(2026, 8, 19, 1, 0, 0, tzinfo=dt.UTC)
    response_time = source_time + dt.timedelta(seconds=26)
    postgres = {
        "session_executions": {
            "rows": [
                {
                    "execution_id": "exe_current",
                    "created_at": source_time + dt.timedelta(milliseconds=480),
                    "completed_at": response_time - dt.timedelta(milliseconds=80),
                },
                {
                    "execution_id": "exe_old",
                    "created_at": source_time - dt.timedelta(hours=8),
                    "completed_at": source_time - dt.timedelta(hours=8) + dt.timedelta(seconds=3),
                },
            ]
        },
        "session_events": {
            "rows": [
                {
                    "execution_id": "exe_old",
                    "event_type": "session.warm_sandbox_claimed",
                    "created_at": source_time - dt.timedelta(hours=8),
                },
                {
                    "execution_id": "exe_current",
                    "event_type": "session.sandbox_resumed",
                    "created_at": source_time + dt.timedelta(seconds=7),
                },
            ]
        },
    }
    discord = {
        "source_message": {"id": "source", "timestamp": source_time.isoformat()},
        "message": {"id": "response", "timestamp": response_time.isoformat(), "content": "ok"},
    }

    analysis = CentaurInvestigatorClient._discord_findings(
        discord=discord,
        postgres=postgres,
        timeline=[],
    )

    assert analysis["warm_pool_hit"] is False
    assert analysis["sandbox_resumed"] is True
    assert "resumed its assigned sandbox" in analysis["summary"]


def test_discord_investigation_can_omit_message_content(monkeypatch) -> None:
    fake = _DiscordFakeConnection()

    async def fake_connect(*args, **kwargs):
        return fake

    monkeypatch.setattr(centaur_client.asyncpg, "connect", fake_connect)
    http_client = _discord_http_client()
    try:
        result = CentaurInvestigatorClient(
            "postgresql://example",
            discord_token="test-token",
            discord_http_client=http_client,
        ).investigate_discord_message(
            "https://discord.com/channels/87003047531651072/"
            "1407795944711524462/1537142385174384830",
            include_observability=False,
            include_content=False,
        )
    finally:
        http_client.close()

    assert "content" not in result["discord"]["message"]
    assert "content" not in result["discord"]["source_message"]


def test_discord_observability_timeline_is_strictly_redacted(monkeypatch) -> None:
    fake = _DiscordFakeConnection()

    async def fake_connect(*args, **kwargs):
        return fake

    class FakeVlogs:
        def hits(self, *args, **kwargs):
            return {"hits": []}

        def field_values(self, *args, **kwargs):
            return []

        def tool_usage_by_thread(self, *args, **kwargs):
            return []

        def query(self, *args, **kwargs):
            return [
                {
                    "_time": "2026-08-12T16:54:56.754Z",
                    "service": "sandbox",
                    "event": "tool_call_completed",
                    "tool_name": "application",
                    "success": "false",
                    "duration_ms": 1359,
                    "tool_args": ["wallet.transfer", {"amount": "100"}],
                    "content": "private Discord content",
                    "error": "credential-shaped error",
                    "authorization": "Bot secret",
                }
            ]

    def fake_load_module(module_name: str, path: Path):
        if "vlogs" in str(path):
            return SimpleNamespace(VictoriaLogsClient=FakeVlogs)
        return None

    monkeypatch.setattr(centaur_client.asyncpg, "connect", fake_connect)
    monkeypatch.setattr(centaur_client, "_safe_load_module", fake_load_module)
    http_client = _discord_http_client()
    try:
        result = CentaurInvestigatorClient(
            "postgresql://example",
            discord_token="test-token",
            discord_http_client=http_client,
        ).investigate_discord_message(
            "https://discord.com/channels/87003047531651072/"
            "1407795944711524462/1537142385174384830",
            include_observability=True,
        )
    finally:
        http_client.close()

    safe_timeline = result["observability"]["vlogs"]["discord_timeline"]
    assert safe_timeline == [
        {
            "_time": "2026-08-12T16:54:56.754Z",
            "service": "sandbox",
            "event": "tool_call_completed",
            "tool_name": "application",
            "success": "false",
            "duration_ms": 1359,
        }
    ]
    serialized = json.dumps(result, default=str)
    assert "wallet.transfer" not in serialized
    assert "private Discord content" not in serialized.replace(
        result["discord"]["message"]["content"], ""
    )
    assert "credential-shaped error" not in serialized
    assert "Bot secret" not in serialized


def test_discord_api_errors_do_not_echo_response_body_or_token() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403,
            json={"code": 50001, "message": "sensitive upstream detail test-token"},
        )

    http_client = httpx.Client(
        base_url=centaur_client.DISCORD_API_URL,
        transport=httpx.MockTransport(handler),
    )
    try:
        result = CentaurInvestigatorClient(
            "postgresql://example",
            discord_token="test-token",
            discord_http_client=http_client,
        ).investigate_discord_message(
            "https://discord.com/channels/87003047531651072/"
            "1407795944711524462/1537142385174384830",
            include_observability=False,
        )
    finally:
        http_client.close()

    assert result == {"status": "error", "error": "Discord API request failed (403, code 50001)"}
    assert "test-token" not in str(result)
