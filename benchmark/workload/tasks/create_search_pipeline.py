from benchmark.workload.task import Task
from typing import Any
from benchmark.basic.my_logger import logger
from benchmark.basic import client


class CreateSearchPipelineTask(Task):
    """Task to create a search pipeline."""
    
    def execute(self) -> Any:
        pipeline_name = self.parameters.get('search-pipeline')
        if not pipeline_name:
            raise ValueError("'search-pipeline' parameter is required")
        
        body = self._load_payload()
        logger.info(f"Creating search pipeline: {pipeline_name}")
        return client.http.put(f"/_search/pipeline/{pipeline_name}", body=body)
