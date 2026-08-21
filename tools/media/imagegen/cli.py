"""CLI for OpenRouter image generation."""

import json
from pathlib import Path
from typing import Annotated

from dotenv import load_dotenv

load_dotenv()

import typer  # noqa: E402
from rich.console import Console  # noqa: E402

from .client import DEFAULT_MODEL  # noqa: E402

app = typer.Typer(name="imagegen", help="Generate and edit images through OpenRouter")
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
    prompt: Annotated[str, typer.Argument(help="Text description of the image to generate")],
    output: Annotated[Path, typer.Option("--output", "-o")] = Path("output.png"),
    model: Annotated[str, typer.Option("--model", "-m")] = DEFAULT_MODEL,
    aspect_ratio: Annotated[str | None, typer.Option("--aspect-ratio", "-a")] = None,
    size: Annotated[str | None, typer.Option("--size", "-s")] = None,
    quality: Annotated[str | None, typer.Option("--quality", "-q")] = None,
    output_format: Annotated[str, typer.Option("--format", "-f")] = "png",
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


@app.command("edit")
def edit(
    reference: Annotated[
        str,
        typer.Argument(help="Local PNG, JPEG, WebP, or HTTPS reference image"),
    ],
    prompt: Annotated[str, typer.Argument(help="Instructions for editing the reference image")],
    output: Annotated[Path, typer.Option("--output", "-o")] = Path("edited.png"),
    model: Annotated[str, typer.Option("--model", "-m")] = DEFAULT_MODEL,
    aspect_ratio: Annotated[str | None, typer.Option("--aspect-ratio", "-a")] = None,
    size: Annotated[str | None, typer.Option("--size", "-s")] = None,
    quality: Annotated[str | None, typer.Option("--quality", "-q")] = None,
    output_format: Annotated[str, typer.Option("--format", "-f")] = "png",
):
    """Edit a local or HTTPS reference image and save the result locally."""
    from .client import ImagegenClient

    with console.status(f"[bold green]Editing image with {model}..."):
        try:
            result = ImagegenClient().generate(
                prompt=prompt,
                output_path=str(output),
                model=model,
                aspect_ratio=aspect_ratio,
                size=size,
                quality=quality,
                output_format=output_format,
                input_references=[reference],
            )
        except Exception as exc:
            console.print(f"[red]Error:[/] {exc}")
            raise typer.Exit(1) from exc
    console.print(
        f"[green]✓[/] Edited image saved to [cyan]{result['image_path']}[/] "
        f"({result['bytes']} bytes)"
    )


if __name__ == "__main__":
    app()
