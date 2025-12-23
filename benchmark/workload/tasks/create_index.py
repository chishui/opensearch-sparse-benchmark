from benchmark.workload.task import Task
from typing import Any
from benchmark.basic.my_logger import logger
from benchmark.basic import client
from benchmark.basic.index import Index


class CreateIndexTask(Task):
    """Task to create an OpenSearch index."""
    
    def execute(self) -> Any:
        index_name = self.parameters.get('index')
        if not index_name:
            raise ValueError("'index' parameter is required for create-index task")
        
        index = Index(index_name)
        # Delete existing index if specified
        if self.config.get('delete', False):
            index.delete()
        
        # Load and create index
        body = self._load_payload()
        logger.info(f"Creating index:{index_name} with body:{body}") 
        index.create(body)
