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
        
        if self.config.get('delete', False):
            logger.info(f"Deleting existing search pipeline: {pipeline_name}")
            try:
                client.http.delete(f"/_search/pipeline/{pipeline_name}")
            except Exception as e:
                logger.warning(f"Failed to delete search pipeline (may not exist): {e}")
        
        body = self._load_payload()
        logger.info(f"Creating search pipeline: {pipeline_name}")
        return client.http.put(f"/_search/pipeline/{pipeline_name}", body=body)
