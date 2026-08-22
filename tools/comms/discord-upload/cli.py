"""CLI for authority-scoped Discord file delivery."""

import json
from pathlib import Path

import typer
from rich.console import Console

app = typer.Typer(
    name="discord-upload",
    help="Upload a file to the current Centaur Discord conversation",
)
console = Console()


@app.command()
def upload(
    file_path: Path = typer.Argument(..., help="Path to the local file to upload"),
    message: str = typer.Option("", "--message", "-m", help="Optional message text"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """Upload FILE to the current Discord conversation."""
    from .client import DiscordUploadClient

    result = DiscordUploadClient().upload(str(file_path), content=message)
    if json_output:
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return
    console.print(
        f"[green]Uploaded[/] {file_path} as message {result.get('id')} "
        f"to channel {result.get('channel_id')}"
    )


if __name__ == "__main__":
    app()
