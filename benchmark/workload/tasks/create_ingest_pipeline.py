from benchmark.workload.task import Task
from typing import Any
from benchmark.basic.my_logger import logger
from benchmark.basic import client


class CreateIngestPipelineTask(Task):
    """Task to create an ingest pipeline."""
    
    def execute(self) -> Any:
        pipeline_name = self.parameters.get('ingest-pipeline')
        if not pipeline_name:
            raise ValueError("'ingest-pipeline' parameter is required")
        
        body = self._load_payload()
        logger.info(f"Creating ingest pipeline: {pipeline_name}")
        return client.http.put(f"/_ingest/pipeline/{pipeline_name}", body=body)
