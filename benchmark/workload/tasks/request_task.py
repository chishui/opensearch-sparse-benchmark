"""General purpose HTTP request task for OpenSearch."""

from typing import Any
from benchmark.workload.task import Task
from benchmark.basic.my_logger import logger
from benchmark.basic import client


class RequestTask(Task):
    """
    General purpose task to make HTTP requests to OpenSearch.
    
    Config example in config.yml:
        - name: custom-request
          type: request
          method: PUT  # GET, POST, PUT, DELETE, HEAD
          url: /my-index/_settings
          payload: settings.json  # optional, for request body
    """
    
    def execute(self, results: list = None) -> Any:
        method = self.config.get('method', 'GET').upper()
        url = self.config.get('url')
        
        if not url:
            raise ValueError("'url' is required for request task")
        
        # Substitute parameters in URL (e.g., /{{index}}/_settings)
        url = self._substitute_params(url)
        
        # Load payload if specified
        body = self._load_payload()
        
        logger.info(f"Executing {method} {url}, with payload {body}")
        
        # Use the low-level transport to make the request
        response = client.transport.perform_request(
            method=method,
            url=url,
            body=body
        )
        
        logger.info(f"Request completed: {method} {url}")
        return response
