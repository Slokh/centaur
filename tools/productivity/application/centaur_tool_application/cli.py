import json
from typing import Annotated

import typer
from centaur_sdk import invoke_application_capability

app = typer.Typer(
    add_completion=False,
    help="Call an allowlisted private application capability through Centaur.",
)


@app.command()
def call(
    capability: Annotated[str, typer.Argument(help="Configured capability name")],
    payload: Annotated[str, typer.Argument(help="JSON object payload")] = "{}",
) -> None:
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as error:
        raise typer.BadParameter(f"payload is not valid JSON: {error}") from error
    if not isinstance(parsed, dict):
        raise typer.BadParameter("payload must be a JSON object")
    try:
        result = invoke_application_capability(capability, parsed)
    except RuntimeError as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(1) from error
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
