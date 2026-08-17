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


def test_generate_sends_local_reference_as_bounded_data_url(
    monkeypatch, tmp_path
) -> None:
    reference = tmp_path / "reference.png"
    reference.write_bytes(b"png-reference")
    seen = {}

    def fake_post(self, url, **kwargs):
        seen["json"] = kwargs["json"]
        return response(
            200,
            {"data": [{"b64_json": base64.b64encode(b"edited").decode()}]},
        )

    monkeypatch.setattr(httpx.Client, "post", fake_post)
    ImagegenClient(api_key="key").generate(
        "add a small red balloon",
        str(tmp_path / "edited.png"),
        input_references=[str(reference)],
    )

    assert seen["json"]["input_references"] == [
        {
            "type": "image_url",
            "image_url": {
                "url": "data:image/png;base64,"
                + base64.b64encode(b"png-reference").decode()
            },
        }
    ]


def test_generate_rejects_unsupported_reference_format(tmp_path) -> None:
    reference = tmp_path / "reference.gif"
    reference.write_bytes(b"gif")
    with pytest.raises(ValueError, match="unsupported reference image format"):
        ImagegenClient(api_key="key").generate(
            "edit it",
            str(tmp_path / "edited.png"),
            input_references=[str(reference)],
        )


def test_generate_accepts_credential_free_https_reference(monkeypatch, tmp_path) -> None:
    seen = {}

    def fake_post(self, url, **kwargs):
        seen["json"] = kwargs["json"]
        return response(
            200,
            {"data": [{"b64_json": base64.b64encode(b"edited").decode()}]},
        )

    monkeypatch.setattr(httpx.Client, "post", fake_post)
    reference = "https://cdn.example.test/image.png?signature=opaque"
    ImagegenClient(api_key="key").generate(
        "edit it",
        str(tmp_path / "edited.png"),
        input_references=[reference],
    )

    assert seen["json"]["input_references"][0]["image_url"]["url"] == reference


@pytest.mark.parametrize(
    "reference",
    [
        "http://example.test/image.png",
        "https://user:password@example.test/image.png",
        "file:///tmp/image.png",
    ],
)
def test_generate_rejects_unsafe_reference_urls(reference, tmp_path) -> None:
    with pytest.raises(ValueError, match="credential-free HTTPS URL"):
        ImagegenClient(api_key="key").generate(
            "edit it",
            str(tmp_path / "edited.png"),
            input_references=[reference],
        )


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
