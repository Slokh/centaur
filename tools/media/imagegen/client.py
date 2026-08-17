"""Client for OpenRouter's dedicated image generation API."""

import base64
import binascii
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from centaur_sdk import secret

BASE_URL = "https://openrouter.ai/api/v1"
DEFAULT_MODEL = "google/gemini-3-pro-image-preview"
MAX_IMAGE_BYTES = 10 * 1024 * 1024
MAX_INPUT_IMAGE_BYTES = 10 * 1024 * 1024
MAX_INPUT_REFERENCES = 4
SUPPORTED_FORMATS = {"png", "jpeg", "webp"}
INPUT_MEDIA_TYPES = {
    ".jpeg": "image/jpeg",
    ".jpg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
}


class ImagegenClient:
    """Generate bounded raster images using an existing OpenRouter credential."""

    def __init__(self, api_key: str | None = None, timeout: float = 180.0):
        self._api_key = api_key
        self.timeout = timeout

    def _headers(self) -> dict[str, str]:
        api_key = self._api_key or secret("OPENROUTER_API_KEY", "")
        if not api_key:
            raise RuntimeError("OPENROUTER_API_KEY not set.")
        return {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    def list_models(self) -> list[dict[str, Any]]:
        """List OpenRouter image models and their advertised capabilities."""
        with httpx.Client(timeout=30.0) as client:
            response = client.get(f"{BASE_URL}/images/models", headers=self._headers())
        self._raise_for_status(response)
        data = response.json().get("data")
        if not isinstance(data, list):
            raise RuntimeError("OpenRouter image models response is missing data.")
        return data

    def generate(
        self,
        prompt: str,
        output_path: str = "output.png",
        model: str = DEFAULT_MODEL,
        aspect_ratio: str | None = None,
        size: str | None = None,
        quality: str | None = None,
        output_format: str = "png",
        input_references: list[str] | None = None,
    ) -> dict[str, Any]:
        """Generate or edit one image and save it to ``output_path``."""
        if not prompt.strip():
            raise ValueError("prompt is required")
        if output_format not in SUPPORTED_FORMATS:
            raise ValueError(f"unsupported output format: {output_format}")

        payload: dict[str, Any] = {
            "model": model,
            "prompt": prompt,
            "n": 1,
            "output_format": output_format,
        }
        if input_references:
            if len(input_references) > MAX_INPUT_REFERENCES:
                raise ValueError(
                    f"at most {MAX_INPUT_REFERENCES} reference images are supported"
                )
            payload["input_references"] = [
                {
                    "type": "image_url",
                    "image_url": {"url": self._reference_image_url(reference)},
                }
                for reference in input_references
            ]
        for name, value in (
            ("aspect_ratio", aspect_ratio),
            ("size", size),
            ("quality", quality),
        ):
            if value:
                payload[name] = value

        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(
                f"{BASE_URL}/images",
                headers=self._headers(),
                json=payload,
            )
        self._raise_for_status(response)
        result = response.json()
        images = result.get("data")
        if not isinstance(images, list) or not images or not isinstance(images[0], dict):
            raise RuntimeError("OpenRouter image response contains no image data.")
        encoded = images[0].get("b64_json")
        if not isinstance(encoded, str) or not encoded:
            raise RuntimeError("OpenRouter image response is missing b64_json.")
        try:
            image = base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError) as error:
            raise RuntimeError("OpenRouter returned invalid base64 image data.") from error
        if len(image) > MAX_IMAGE_BYTES:
            raise RuntimeError(
                f"Generated image is too large ({len(image)} bytes > {MAX_IMAGE_BYTES} byte limit)."
            )

        output = Path(output_path).expanduser()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(image)
        usage = result.get("usage") if isinstance(result.get("usage"), dict) else {}
        return {
            "status": "ok",
            "image_path": str(output),
            "model": model,
            "media_type": images[0].get("media_type") or f"image/{output_format}",
            "bytes": len(image),
            "cost": usage.get("cost"),
        }

    @staticmethod
    def _reference_image_url(raw_reference: str) -> str:
        parsed = urlparse(raw_reference)
        if parsed.scheme:
            if (
                parsed.scheme != "https"
                or not parsed.hostname
                or parsed.username is not None
                or parsed.password is not None
                or len(raw_reference) > 4_096
            ):
                raise ValueError("reference image URL must be a bounded credential-free HTTPS URL")
            return raw_reference

        raw_path = raw_reference
        path = Path(raw_path).expanduser()
        media_type = INPUT_MEDIA_TYPES.get(path.suffix.lower())
        if media_type is None:
            raise ValueError(
                f"unsupported reference image format: {path.suffix or '(none)'}"
            )
        try:
            size = path.stat().st_size
        except OSError as error:
            raise ValueError(f"could not read reference image: {path}") from error
        if size <= 0:
            raise ValueError(f"reference image is empty: {path}")
        if size > MAX_INPUT_IMAGE_BYTES:
            raise ValueError(
                f"reference image is too large ({size} bytes > "
                f"{MAX_INPUT_IMAGE_BYTES} byte limit)"
            )
        try:
            encoded = base64.b64encode(path.read_bytes()).decode("ascii")
        except OSError as error:
            raise ValueError(f"could not read reference image: {path}") from error
        return f"data:{media_type};base64,{encoded}"

    @staticmethod
    def _raise_for_status(response: httpx.Response) -> None:
        if response.status_code < 400:
            return
        try:
            payload = response.json()
            error = payload.get("error")
            if isinstance(error, dict):
                message = error.get("message") or str(error)
            else:
                message = error or payload.get("message") or response.text
        except Exception:
            message = response.text
        raise RuntimeError(f"OpenRouter image API error ({response.status_code}): {message}")


def _client() -> ImagegenClient:
    return ImagegenClient()
