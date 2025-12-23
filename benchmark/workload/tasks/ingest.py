
from benchmark.workload.task import Task
from typing import Any
from benchmark.basic.my_logger import logger
from benchmark.basic import client


class IngestTask(Task):
    """Task to ingest documents into an index."""
    
    def execute(self) -> Any:
        index_name = self.parameters.get('index')
        if not index_name:
            raise ValueError("'index' parameter is required for ingest task")
        
        doc_generator = self._load_script()
        if not doc_generator:
            raise ValueError("couldn't find doc_generator function from python script")
        
        bulk_size = self.parameters.get('bulk_size', 100)
        doc_count = 0
        bulk_body = []
        
        logger.info(f"Starting ingestion to index: {index_name}")
        runner = self.parameters.get('runner')
        if not runner:
            raise ValueError("must set runner in global params to run ingest")

        clients = self.parameters.get('clients')
        if not clients:
            clients = 4
            print(f"use default client size: {clients}, set `clients` parameter in command to customize")
        else:
            clients = int(clients)

        total_count = self.parameters.get('total_count')
        if total_count:
            total_count = int(total_count)

        metrics = runner.run(
            data_generator=doc_generator(),
            user_count=clients,        # number of concurrent Locust users
            spawn_rate=1,        # users spawned per second
            wait_for_completion=True,
            total_count=total_count
        )
        runner.print_report(metrics)
        return metrics
