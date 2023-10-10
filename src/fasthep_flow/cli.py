from __future__ import annotations

from pathlib import Path

import typer

from .config import load_config

app = typer.Typer()


@app.command()
def lint(config: Path) -> None:
    typer.echo(f"Linting {config}")
    load_config(config)
    typer.echo("Looks good to me!")


@app.command()
def print_defaults() -> None:
    typer.echo("Printing defaults...")


@app.command()
def execute(config: Path) -> None:
    typer.echo(f"Executing {config}...")


def main() -> None:
    app()
