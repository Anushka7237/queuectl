from __future__ import annotations
from pathlib import Path

import json
import sys
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from queuectl.config import CONFIG_KEYS, get_data_dir
from queuectl.models import JobState
from queuectl.queue import QueueService
from queuectl.settings import ConfigManager

app = typer.Typer(
    name="queuectl",
    help="CLI-based background job queue with retries, DLQ, and persistence.",
    no_args_is_help=True,
)
config_app = typer.Typer(help="Manage queue configuration.")
worker_app = typer.Typer(help="Manage worker processes.")
dlq_app = typer.Typer(help="Dead Letter Queue operations.")
app.add_typer(config_app, name="config")
app.add_typer(worker_app, name="worker")
app.add_typer(dlq_app, name="dlq")

console = Console()
stderr = Console(file=sys.stderr)


def _service() -> QueueService:
    return QueueService()


def _print_json(data: object) -> None:
    console.print_json(json.dumps(data, default=str))


def _print_jobs(jobs: list) -> None:
    if not jobs:
        console.print("[dim]No jobs found.[/dim]")
        return

    table = Table(show_header=True, header_style="bold")
    table.add_column("ID")
    table.add_column("State")
    table.add_column("Attempts")
    table.add_column("Command")
    table.add_column("Updated At")

    for job in jobs:
        table.add_row(
            job.id,
            job.state.value,
            str(job.attempts),
            job.command,
            job.updated_at.isoformat(),
        )
    console.print(table)


@app.command()
def enqueue(
    job_json: str = typer.Argument(
        ...,
        help='Job JSON string or path to a JSON file',
    ),
) -> None:
    """Add a new job to the queue."""
    try:
        path = Path(job_json)
        if path.is_file():
            payload = json.loads(path.read_text(encoding="utf-8"))
        else:
            payload = json.loads(job_json)
        if not isinstance(payload, dict):
            raise ValueError("Job payload must be a JSON object")
        job = _service().enqueue(payload)
        console.print(f"[green]Enqueued job[/green] {job.id}")
        _print_json(job.to_dict())
    except json.JSONDecodeError as exc:
        stderr.print(f"[red]Invalid JSON:[/red] {exc}")
        raise typer.Exit(code=1)
    except ValueError as exc:
        stderr.print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(code=1)

@app.command("list")
def list_jobs(
    state: Optional[str] = typer.Option(
        None,
        "--state",
        help="Filter by state: pending, processing, completed, failed, dead",
    ),
) -> None:
    """List jobs, optionally filtered by state."""
    if state:
        try:
            JobState(state)
        except ValueError:
            stderr.print(f"[red]Invalid state:[/red] {state}")
            raise typer.Exit(code=1)

    jobs = _service().list_jobs(state)
    _print_jobs(jobs)


@app.command()
def status() -> None:
    """Show summary of job states and configuration."""
    summary = _service().get_status()
    table = Table(title="Queue Status", show_header=True, header_style="bold")
    table.add_column("State")
    table.add_column("Count", justify="right")

    for job_state in JobState:
        table.add_row(job_state.value, str(summary["jobs"].get(job_state.value, 0)))
    table.add_row("[bold]Total[/bold]", str(summary["total"]))
    console.print(table)

    config_table = Table(title="Configuration", show_header=True, header_style="bold")
    config_table.add_column("Key")
    config_table.add_column("Value", justify="right")
    for key, value in summary["config"].items():
        config_table.add_row(key, str(value))
    console.print(config_table)

    worker_line = (
        f"[green]running[/green] (pid {summary['worker_pid']})"
        if summary["worker_pid"]
        else "[dim]not running[/dim]"
    )
    console.print(f"Workers: {worker_line}")
    console.print(f"[dim]Data directory:[/dim] {get_data_dir()}")


@config_app.command("set")
def config_set(
    key: str = typer.Argument(..., help="Config key: max-retries or backoff-base"),
    value: int = typer.Argument(..., help="Integer value"),
) -> None:
    """Set a configuration value."""
    if key not in CONFIG_KEYS:
        stderr.print(f"[red]Unknown config key:[/red] {key}")
        stderr.print(f"Valid keys: {', '.join(CONFIG_KEYS)}")
        raise typer.Exit(code=1)

    try:
        ConfigManager().set_value(key, value)
        console.print(f"[green]Set[/green] {key} = {value}")
    except ValueError as exc:
        stderr.print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(code=1)


@config_app.command("show")
def config_show() -> None:
    """Show current configuration."""
    _print_json(ConfigManager().as_display_dict())


@worker_app.command("start")
def worker_start(
    count: int = typer.Option(1, "--count", min=1, help="Number of worker threads"),
) -> None:
    """Start workers to process jobs from the queue."""
    from queuectl.worker import WorkerManager, install_signal_handlers, read_worker_pid

    existing_pid = read_worker_pid()
    if existing_pid is not None:
        stderr.print(f"[red]Workers already running[/red] (pid {existing_pid})")
        raise typer.Exit(code=1)

    service = _service()
    manager = WorkerManager(
        storage=service.storage,
        config=service.config,
        count=count,
    )
    install_signal_handlers(manager)
    console.print(f"[green]Starting {count} worker(s)...[/green] Press Ctrl+C to stop gracefully.")
    manager.start()
    console.print("[yellow]Workers stopped.[/yellow]")


@worker_app.command("stop")
def worker_stop() -> None:
    """Signal running workers to stop after finishing current jobs."""
    from queuectl.worker import WorkerManager, read_worker_pid

    pid = read_worker_pid()
    if pid is None:
        stderr.print("[yellow]No running workers found.[/yellow]")
        raise typer.Exit(code=1)

    manager = WorkerManager(storage=_service().storage, config=ConfigManager())
    manager.request_stop()
    console.print(f"[green]Stop signal sent[/green] to worker process (pid {pid})")


@dlq_app.command("list")
def dlq_list() -> None:
    """List jobs in the Dead Letter Queue."""
    jobs = _service().list_dlq()
    if not jobs:
        console.print("[dim]Dead letter queue is empty.[/dim]")
        return
    _print_jobs(jobs)


@dlq_app.command("retry")
def dlq_retry(
    job_id: str = typer.Argument(..., help="ID of the dead job to retry"),
) -> None:
    """Move a dead job back to the pending queue."""
    try:
        job = _service().retry_dlq_job(job_id)
        console.print(f"[green]Requeued job[/green] {job.id}")
        _print_json(job.to_dict())
    except ValueError as exc:
        stderr.print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(code=1)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
