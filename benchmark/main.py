"""OpenSearch benchmark CLI."""

import click
from pathlib import Path

from benchmark.basic.my_logger import logger
from benchmark.workload.workload import Workload
from benchmark.locust.locust_manager import LocustManager

@click.group()
def cli():
    """OpenSearch benchmark tool."""


def parse_parameters(ctx, param, value):
    """Parse key=value pairs into a dictionary."""
    if not value:
        return {}
    params = {}
    for item in value:
        if '=' not in item:
            raise click.BadParameter(f"Invalid format '{item}'. Use key=value")
        key, val = item.split('=', 1)
        params[key] = val
    return params


@cli.command()
@click.option("--workload", "-w", required=True, help="Path to workload directory (e.g., workloads/msmarco_v2)")
@click.option("--workload-parameters", "-p", multiple=True, callback=parse_parameters,
              help="Runtime parameters as key=value pairs (can be used multiple times)")
@click.option("--skip-tasks", "-s", multiple=True, help="Task names to skip (can be used multiple times)")
def run(workload: str, workload_parameters: dict, skip_tasks: tuple):
    """Run a benchmark workload."""
    workload_path = Path(workload)

    if not workload_path.exists():
        logger.error(f"Workload path not found: {workload_path}")
        return

    # Create workload with runtime parameters (global_params ready immediately)
    wl = Workload(workload_path, runtime_params=workload_parameters)
    
    # Parse tasks
    wl.parse()

    # Execute all tasks
    results = wl.run(skip_tasks=set(skip_tasks))

    # Print summary
    success_count = sum(1 for r in results if r['status'] == 'success')
    logger.info(f"\nCompleted: {success_count}/{len(results)} tasks succeeded")


if __name__ == "__main__":
    cli()
