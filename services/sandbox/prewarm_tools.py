#!/usr/bin/env python3
"""Materialize configured tool environments before a sandbox becomes ready."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from collections.abc import Callable, Iterable

TOOL_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
DEFAULT_TIMEOUT_SECONDS = 60


def parse_tool_names(raw: str) -> list[str]:
    names: list[str] = []
    for name in re.split(r"[\s,]+", raw.strip()):
        if not name:
            continue
        if not TOOL_NAME.fullmatch(name):
            raise ValueError(f"invalid prewarm tool name: {name!r}")
        if name not in names:
            names.append(name)
    return names


def _invoke_help(executable: str, timeout_seconds: int) -> None:
    subprocess.run(
        [executable, "--help"],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=timeout_seconds,
    )


def prewarm_tools(
    names: Iterable[str],
    timeout_seconds: int,
    *,
    resolve: Callable[[str], str | None] = shutil.which,
    invoke: Callable[[str, int], None] = _invoke_help,
) -> None:
    for name in names:
        executable = resolve(name)
        if executable is None:
            raise RuntimeError(f"configured prewarm tool is unavailable: {name}")
        try:
            invoke(executable, timeout_seconds)
        except subprocess.TimeoutExpired as error:
            raise RuntimeError(
                f"prewarm timed out for {name} after {timeout_seconds}s"
            ) from error
        except subprocess.CalledProcessError as error:
            raise RuntimeError(
                f"prewarm failed for {name} with exit code {error.returncode}"
            ) from error
        print(f"prewarmed tool: {name}", file=sys.stderr)


def _timeout_seconds(raw: str) -> int:
    try:
        timeout = int(raw)
    except ValueError as error:
        raise ValueError(
            "CENTAUR_TOOL_PREWARM_TIMEOUT_SECONDS must be an integer"
        ) from error
    if not 1 <= timeout <= 600:
        raise ValueError(
            "CENTAUR_TOOL_PREWARM_TIMEOUT_SECONDS must be between 1 and 600"
        )
    return timeout


def main() -> int:
    try:
        names = parse_tool_names(os.environ.get("CENTAUR_TOOL_PREWARM", "application"))
        timeout_seconds = _timeout_seconds(
            os.environ.get(
                "CENTAUR_TOOL_PREWARM_TIMEOUT_SECONDS",
                str(DEFAULT_TIMEOUT_SECONDS),
            )
        )
        prewarm_tools(names, timeout_seconds)
    except (RuntimeError, ValueError) as error:
        print(f"tool prewarm error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
