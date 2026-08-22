import json
import os
import sys
from typing import Annotated
from urllib.parse import quote

import httpx
import typer

app = typer.Typer(
    add_completion=False,
    help="Call an allowlisted private application capability through Centaur.",
)


@app.command()
def call(
    capability: Annotated[str, typer.Argument(help="Configured capability name")],
    payload: Annotated[str, typer.Argument(help="JSON object payload")] = "{}",
) -> None:
    thread_key = os.environ.get("CENTAUR_THREAD_KEY", "").strip()
    api_url = os.environ.get(
        "CENTAUR_API_URL", "http://centaur-api-rs:8080"
    ).rstrip("/")
    if not thread_key:
        raise typer.BadParameter("CENTAUR_THREAD_KEY is not configured")
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as error:
        raise typer.BadParameter(f"payload is not valid JSON: {error}") from error
    if not isinstance(parsed, dict):
        raise typer.BadParameter("payload must be a JSON object")
    response = httpx.post(
        f"{api_url}/api/session/{quote(thread_key, safe='')}/application/{quote(capability, safe='')}",
        json=parsed,
        timeout=30,
    )
    if response.status_code >= 400:
        print(response.text, file=sys.stderr)
        raise typer.Exit(1)
    print(response.text)
