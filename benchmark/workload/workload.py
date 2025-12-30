"""Workload parser and executor for OpenSearch benchmark."""

import re
import json
import time
import yaml
import importlib.util
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable
from benchmark.workload.task import Task
from benchmark.workload.tasks.ingest import IngestTask
from benchmark.workload.tasks.task_factory import TaskFactory
from benchmark.basic import client
from benchmark.basic.my_logger import logger


class Workload:
    """Parses and executes OpenSearch benchmark workloads."""
    
    def __init__(self, workload_path: str):
        self.workload_path = Path(workload_path)
        self.config_path = self.workload_path / 'config.yml'
        self.config: Dict[str, Any] = {}
        self.tasks: List[Task] = []
        self.global_params: Dict[str, Any] = {}
        self.runner = None
    
    def bind_runner(self, runner, name):
        for task in self.tasks:
            if task.name == name:
                task.parameters['runner'] = runner

    def parse(self, runtime_params: Dict[str, Any] = None) -> 'Workload':
        """Parse the config.yml and generate task list.
        
        Args:
            runtime_params: Optional runtime parameters that override global parameters
        """
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        with open(self.config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

        # Extract global parameters and merge with runtime params (runtime overrides config)
        self.global_params = self.config.get('parameters', {}) or {}
        if runtime_params:
            self.global_params.update(runtime_params)
        
        # Parse tasks
        task_configs = self.config.get('tasks', [])
        for task_config in task_configs:
            task_name = task_config.get('name')
            if not task_name:
                raise ValueError("Each task must have a 'name' field")
            type_name = task_config.get('type')
            
            task = TaskFactory.create(
                name=task_name,
                type_name=type_name,
                config=task_config,
                global_params=self.global_params,
                workload_dir=self.workload_path
            )
            self.tasks.append(task)

        logger.info(f"Parsed {len(self.tasks)} tasks from {self.config_path}")
        return self

    def run(self, skip_tasks: set = None) -> List[Dict[str, Any]]:
        """Execute all tasks sequentially.
        
        Args:
            skip_tasks: Set of task names to skip
        """
        results = []
        skip_tasks = skip_tasks or set()

        for i, task in enumerate(self.tasks, 1):
            if task.name in skip_tasks:
                logger.info(f"[{i}/{len(self.tasks)}] Skipping task: {task.name}")
                results.append({
                    'task': task.name,
                    'status': 'skipped'
                })
                continue

            logger.info(f"[{i}/{len(self.tasks)}] Executing task: {task.name}")
            try:
                start_time = time.time()
                result = task.execute()
                execution_time = time.time() - start_time
                results.append({
                    'task': task.name,
                    'status': 'success',
                    'result': result,
                    'execution_time': execution_time
                })
                logger.info(f"Task '{task.name}' completed successfully in {execution_time:.2f}s")
                
                # Sleep after task completion if specified
                sleep_seconds = task.config.get('sleep')
                if sleep_seconds:
                    logger.info(f"Sleeping for {sleep_seconds} seconds after task '{task.name}'")
                    time.sleep(sleep_seconds)
            except Exception as e:
                import traceback
                execution_time = time.time() - start_time
                logger.error(f"Task '{task.name}' failed after {execution_time:.2f}s")
                traceback.print_exc()
                results.append({
                    'task': task.name,
                    'status': 'failed',
                    'error': str(e),
                    'execution_time': execution_time
                })
                # Optionally stop on first failure
                if self.config.get('stop_on_failure', True):
                    break
        
        return results
