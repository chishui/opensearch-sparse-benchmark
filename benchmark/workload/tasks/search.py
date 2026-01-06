"""Search task for executing search queries against OpenSearch."""

from typing import Any, Dict
from benchmark.workload.tasks.runner_task import RunnerTask
from benchmark.basic.my_logger import logger


class SearchTask(RunnerTask):
    """Task to execute search queries."""

    def execute(self, results: list = None) -> Any:
        index_name = self.parameters.get('index')
        if not index_name:
            raise ValueError("'index' parameter is required for search task")

        doc_generator = self._load_script()
        if not doc_generator:
            raise ValueError(
                "couldn't find doc_generator function from python script"
            )

        logger.info("Starting search on index: %s", index_name)
        if not self.runner:
            raise ValueError("must set runner in global params to run search")

        clients = self.parameters.get('clients')
        if not clients:
            clients = 4
            print(
                f"use default client size: {clients}, "
                "set `clients` parameter in command to customize"
            )
        else:
            clients = int(clients)

        total_count = self.parameters.get('total_count')
        doc_generator_kwargs = {}
        if total_count:
            total_count = int(total_count)
            doc_generator_kwargs = {"total_count": total_count}

        metrics = self.runner.run(
            data_generator=doc_generator(**doc_generator_kwargs),
            user_count=clients,
            spawn_rate=1,
            wait_for_completion=True,
            total_count=total_count
        )
        self.print_report(metrics)
        return metrics

