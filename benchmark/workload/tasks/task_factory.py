from benchmark.workload.tasks.create_index import CreateIndexTask
from benchmark.workload.tasks.create_search_pipeline import CreateSearchPipelineTask
from benchmark.workload.tasks.create_ingest_pipeline import CreateIngestPipelineTask
from benchmark.workload.tasks.ingest import IngestTask
from benchmark.workload.tasks.request_task import RequestTask
from benchmark.workload.tasks.search import SearchTask
from benchmark.workload.task import Task
from typing import Any, Dict, List, Optional, Callable
from pathlib import Path

class TaskFactory:
    """Factory to create task instances based on task name."""
    
    TASK_TYPES = {
        'create-index': CreateIndexTask,
        'create-ingest-pipeline': CreateIngestPipelineTask,
        'create-search-pipeline': CreateSearchPipelineTask,
        'ingest': IngestTask,
        'request': RequestTask,
        'search': SearchTask
    }
    
    @classmethod
    def create(cls, name: str, type_name: str, config: Dict[str, Any], global_params: Dict[str, Any], workload_dir: Path) -> Task:
        task_class =  cls.TASK_TYPES.get(type_name) if type_name is not None else cls.TASK_TYPES.get(name)
        if not task_class:
            raise ValueError(f"Unknown task type: {name}, {type_name}")
        return task_class(name, config, global_params, workload_dir)

