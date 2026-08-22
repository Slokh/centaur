"""Authority-scoped Discord file delivery client."""

import json
import os
from typing import Any

import httpx

from centaur_sdk import current_scoped_discord_thread, secret

BASE_URL = "https://discord.com/api/v10"
USER_AGENT = "DiscordBot (https://github.com/paradigmxyz/centaur, 0.1.0)"
MAX_UPLOAD_BYTES = 10 * 1024 * 1024


class DiscordUploadClient:
    """Upload only to the Discord destination owned by the active session."""

    def __init__(self, token: str | None = None, timeout: float = 30.0):
        self._token = token
        self.timeout = timeout

    def _get_token(self) -> str:
        token = self._token or secret("DISCORD_BOT_TOKEN", "")
        if not token:
            raise RuntimeError("DISCORD_BOT_TOKEN not set.")
        return token

    def upload(self, file_path: str, content: str = "") -> dict[str, Any]:
        """Upload a local file to the active Discord conversation."""
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        size = os.path.getsize(file_path)
        if size > MAX_UPLOAD_BYTES:
            raise ValueError(
                f"File is too large to upload ({size} bytes > {MAX_UPLOAD_BYTES} byte limit)."
            )

        destination = current_scoped_discord_thread()
        channel_id = destination.get("thread_id") or destination["channel_id"]
        reply_to = destination.get("reply_to_message_id")
        payload: dict[str, Any] = {}
        if content:
            payload["content"] = content
        if reply_to:
            payload["message_reference"] = {"message_id": reply_to}

        headers = {
            "Authorization": self._get_token(),
            "User-Agent": USER_AGENT,
        }
        with open(file_path, "rb") as handle:
            files = {"files[0]": (os.path.basename(file_path), handle)}
            with httpx.Client(timeout=self.timeout) as client:
                response = client.post(
                    f"{BASE_URL}/channels/{channel_id}/messages",
                    headers=headers,
                    data={"payload_json": json.dumps(payload)},
                    files=files,
                )
        if response.status_code >= 400:
            try:
                message = response.json().get("message", response.text)
            except Exception:
                message = response.text
            raise RuntimeError(f"Discord API error ({response.status_code}): {message}")
        result = response.json()
        return {
            "id": str(result.get("id", "")),
            "channel_id": str(result.get("channel_id", channel_id)),
            "filename": os.path.basename(file_path),
        }


def _client() -> DiscordUploadClient:
    return DiscordUploadClient()
