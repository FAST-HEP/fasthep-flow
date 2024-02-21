from __future__ import annotations

from pathlib import Path

import hydra
import typer
from typing_extensions import Annotated

from .config import FlowConfig, load_config

app = typer.Typer()


def init_config(config: Path, overrides: list[str] | None = None) -> FlowConfig:
    """Glue function to bring hydra and typer togeother
    see https://github.com/facebookresearch/hydra/issues/1964
    and
    https://stackoverflow.com/questions/70811640/using-typer-and-hydra-together"""
    # get the directory the config file is in
    parent: str = str(Path(config).parent.resolve())
    hydra.initialize_config_dir(config_dir=parent)
    # merge config with overrides parameters passed in the command line
    cfg = hydra.compose(config_name=Path(config).stem, overrides=overrides)
    return FlowConfig.from_dictconfig(cfg)


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


@app.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True}
)
def execute(
    config: Path, overrides: Annotated[list[str] | None, typer.Argument()] = None
) -> None:
    """Execute a config file."""
    typer.echo(f"Executing {config}...")

    # initialize hydra
    cfg = init_config(config, overrides)
    typer.echo(cfg)


@app.command("list")
def list_stuff(what: str) -> None:
    """List available operators, tasks, or flows."""
    # TODO: also list available config options
    # TODO: typer has a way to limit the choices, use that
    typer.echo(f"Listing {what}...")


def main() -> None:
    """Entrypoint for the CLI."""
    app()
