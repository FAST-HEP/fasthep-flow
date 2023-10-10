from __future__ import annotations

from pathlib import Path

import typer

from .config import load_config

app = typer.Typer()


@app.command()
def lint(config: Path) -> None:
    """Lint a config file. Throws errors if the config is invalid."""
    typer.echo(f"Linting {config}")
    load_config(config)
    typer.echo("Looks good to me!")


@app.command()
def print_defaults() -> None:
    """Print the default config."""
    typer.echo("Printing defaults...")


@app.command()
def execute(config: Path) -> None:
    """Execute a config file."""
    typer.echo(f"Executing {config}...")


def main() -> None:
    """Entrypoint for the CLI."""
    app()
