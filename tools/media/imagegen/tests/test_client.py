import base64
import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from client import ImagegenClient


def response(status: int, payload: dict) -> httpx.Response:
    return httpx.Response(
        status,
        json=payload,
        request=httpx.Request("POST", "https://openrouter.ai/api/v1/images"),
    )


def test_generate_calls_dedicated_image_api_and_writes_image(monkeypatch, tmp_path) -> None:
    output = tmp_path / "generated.png"
    seen = {}

    def fake_post(self, url, **kwargs):
        seen["url"] = url
        seen["headers"] = kwargs["headers"]
        seen["json"] = kwargs["json"]
        return response(
            200,
            {
                "data": [{"b64_json": base64.b64encode(b"png-bytes").decode()}],
                "usage": {"cost": 0.04},
            },
        )

    monkeypatch.setattr(httpx.Client, "post", fake_post)
    result = ImagegenClient(api_key="key").generate(
        "A small blue robot",
        output_path=str(output),
        aspect_ratio="1:1",
        size="1K",
    )

    assert seen["url"] == "https://openrouter.ai/api/v1/images"
    assert seen["headers"]["Authorization"] == "Bearer key"
    assert seen["json"] == {
        "model": "google/gemini-3-pro-image-preview",
        "prompt": "A small blue robot",
        "n": 1,
        "output_format": "png",
        "aspect_ratio": "1:1",
        "size": "1K",
    }
    assert output.read_bytes() == b"png-bytes"
    assert result["cost"] == 0.04


def test_generate_rejects_missing_image_data(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        httpx.Client,
        "post",
        lambda self, url, **kwargs: response(200, {"data": []}),
    )
    with pytest.raises(RuntimeError, match="no image data"):
        ImagegenClient(api_key="key").generate("robot", str(tmp_path / "out.png"))


def test_generate_rejects_invalid_base64(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        httpx.Client,
        "post",
        lambda self, url, **kwargs: response(200, {"data": [{"b64_json": "not base64"}]}),
    )
    with pytest.raises(RuntimeError, match="invalid base64"):
        ImagegenClient(api_key="key").generate("robot", str(tmp_path / "out.png"))


def test_generate_surfaces_provider_error(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        httpx.Client,
        "post",
        lambda self, url, **kwargs: response(
            400, {"error": {"message": "model does not support size"}}
        ),
    )
    with pytest.raises(RuntimeError, match="model does not support size"):
        ImagegenClient(api_key="key").generate("robot", str(tmp_path / "out.png"))
