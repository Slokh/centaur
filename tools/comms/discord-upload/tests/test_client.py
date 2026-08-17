import json
import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from client import DiscordUploadClient


def test_upload_uses_session_reply_destination(monkeypatch, tmp_path) -> None:
    image = tmp_path / "generated.png"
    image.write_bytes(b"png")
    seen = {}

    monkeypatch.setattr(
        "client.current_scoped_discord_thread",
        lambda: {
            "guild_id": "111",
            "channel_id": "222",
            "reply_to_message_id": "444",
        },
    )

    def fake_post(self, url, **kwargs):
        seen["url"] = url
        seen["payload"] = json.loads(kwargs["data"]["payload_json"])
        seen["filename"] = kwargs["files"]["files[0]"][0]
        return httpx.Response(
            200,
            json={"id": "555", "channel_id": "222"},
            request=httpx.Request("POST", url),
        )

    monkeypatch.setattr(httpx.Client, "post", fake_post)

    result = DiscordUploadClient(token="Bot token").upload(str(image), "Here it is")

    assert seen == {
        "url": "https://discord.com/api/v10/channels/222/messages",
        "payload": {
            "content": "Here it is",
            "message_reference": {"message_id": "444"},
        },
        "filename": "generated.png",
    }
    assert result == {"id": "555", "channel_id": "222", "filename": "generated.png"}


def test_upload_uses_thread_channel_without_cross_channel_input(monkeypatch, tmp_path) -> None:
    image = tmp_path / "generated.png"
    image.write_bytes(b"png")
    seen = {}
    monkeypatch.setattr(
        "client.current_scoped_discord_thread",
        lambda: {"guild_id": "111", "channel_id": "222", "thread_id": "333"},
    )

    def fake_post(self, url, **kwargs):
        seen["url"] = url
        return httpx.Response(
            200,
            json={"id": "555", "channel_id": "333"},
            request=httpx.Request("POST", url),
        )

    monkeypatch.setattr(httpx.Client, "post", fake_post)
    DiscordUploadClient(token="Bot token").upload(str(image))
    assert seen["url"].endswith("/channels/333/messages")


def test_upload_rejects_missing_file(monkeypatch) -> None:
    monkeypatch.setattr(
        "client.current_scoped_discord_thread",
        pytest.fail,
    )
    with pytest.raises(FileNotFoundError):
        DiscordUploadClient(token="Bot token").upload("missing.png")


def test_upload_rejects_oversized_file(monkeypatch, tmp_path) -> None:
    oversized = tmp_path / "oversized.png"
    oversized.write_bytes(b"")
    monkeypatch.setattr("client.os.path.getsize", lambda _path: 10 * 1024 * 1024 + 1)
    monkeypatch.setattr(
        "client.current_scoped_discord_thread",
        pytest.fail,
    )
    with pytest.raises(ValueError, match="too large"):
        DiscordUploadClient(token="Bot token").upload(str(oversized))
