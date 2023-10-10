import typer
from pathlib import Path

app = typer.Typer()

from .config import load_config

@app.command()
def lint(config: Path):
    typer.echo(f"Linting {config}")
    load_config(config)
    typer.echo("Looks good to me!")

@app.command()
def print_defaults():
    typer.echo("Printing defaults...")

@app.command()
def execute(config: Path):
    typer.echo(f"Executing {config}...")

def main():
    app()
