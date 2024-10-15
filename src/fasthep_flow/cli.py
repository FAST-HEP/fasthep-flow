"""Command line interface for fasthep-flow."""
from __future__ import annotations

from pathlib import Path

import hydra
import typer

from .config import FlowConfig, load_config
from .orchestration import workflow_to_hamilton_dag
from .workflow import Workflow

app = typer.Typer()


def init_config(config: Path, overrides: list[str] | None = None) -> FlowConfig:
    """Glue function to bring hydra and typer togeother
    see https://github.com/facebookresearch/hydra/issues/1964
    and
    https://stackoverflow.com/questions/70811640/using-typer-and-hydra-together"""
    # get the directory the config file is in
    parent: str = str(Path(config).parent.resolve())
    config_name = Path(config).stem
    hydra.initialize_config_dir(config_dir=parent, version_base="1.1")
    # merge config with overrides parameters passed in the command line
    cfg = hydra.compose(config_name=config_name, overrides=overrides)
    flow = FlowConfig.from_dictconfig(cfg)
    flow.metadata = {"config_file": str(config), "name": config_name}
    return flow


@app.command()
def lint(config: Path) -> None:
    """Lint a config file. Throws errors if the config is invalid."""
    typer.echo(f"Linting {config}")
    try:
        load_config(config)
    except Exception as e:
        typer.echo(f"Error: {e}")
        raise typer.Exit(code=1) from e
    typer.echo("Looks good to me!")


@app.command()
def print_defaults() -> None:
    """Print the default config."""
    typer.echo("Printing defaults...")


@app.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True}
)
def execute(
    config: Path,
    overrides: list[str] | None = None,
    save_path: str = "~/.fasthep/flow/",
) -> None:
    """Execute a config file."""
    typer.echo(f"Executing {config}...")

    cfg = init_config(config, overrides)
    workflow = Workflow(config=cfg)
    save_path = workflow.save(Path(save_path))
    dag = workflow_to_hamilton_dag(workflow, save_path)
    dag.visualize_execution(
        final_vars=workflow.task_names,
        output_file_path=Path(workflow.save_path) / "dag.png",
    )
    # TODO: if specified, run a specific task/node with execute_node
    results = dag.execute(workflow.task_names, inputs={})
    typer.echo(f"Results: {results}")


@app.command("list")
def list_stuff(what: str) -> None:
    """List available operators, tasks, or flows."""
    # TODO: also list available config options
    # TODO: typer has a way to limit the choices, use that
    typer.echo(f"Listing {what}...")


def main() -> None:
    """Entrypoint for the CLI."""
    app()
