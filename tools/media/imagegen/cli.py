"""CLI for OpenRouter image generation."""

import json
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

import typer  # noqa: E402
from rich.console import Console  # noqa: E402

from .client import DEFAULT_MODEL  # noqa: E402

app = typer.Typer(name="imagegen", help="Generate images through OpenRouter")
console = Console()


@app.command("health")
def health():
    """Assert image model discovery and authentication."""
    from .client import ImagegenClient

    try:
        models = ImagegenClient().list_models()
        details = {"models": [model.get("id") for model in models[:3]]}
        payload = {"ok": True, "tool": "imagegen", "error": None, "details": details}
    except Exception as exc:
        payload = {"ok": False, "tool": "imagegen", "error": str(exc), "details": {}}
        print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
        raise typer.Exit(1) from exc
    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))


@app.command("generate")
def generate(
    prompt: str = typer.Argument(..., help="Text description of the image to generate"),
    output: Path = typer.Option(Path("output.png"), "--output", "-o"),
    model: str = typer.Option(DEFAULT_MODEL, "--model", "-m"),
    aspect_ratio: str = typer.Option(None, "--aspect-ratio", "-a"),
    size: str = typer.Option(None, "--size", "-s"),
    quality: str = typer.Option(None, "--quality", "-q"),
    output_format: str = typer.Option("png", "--format", "-f"),
):
    """Generate one image and save it locally."""
    from .client import ImagegenClient

    with console.status(f"[bold green]Generating image with {model}..."):
        try:
            result = ImagegenClient().generate(
                prompt=prompt,
                output_path=str(output),
                model=model,
                aspect_ratio=aspect_ratio,
                size=size,
                quality=quality,
                output_format=output_format,
            )
        except Exception as exc:
            console.print(f"[red]Error:[/] {exc}")
            raise typer.Exit(1) from exc
    console.print(
        f"[green]✓[/] Image saved to [cyan]{result['image_path']}[/] "
        f"({result['bytes']} bytes)"
    )


if __name__ == "__main__":
    app()
