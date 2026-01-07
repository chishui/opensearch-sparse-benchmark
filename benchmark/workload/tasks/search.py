"""Search task for executing search queries against OpenSearch."""

import os
from typing import Any, Dict
from pathlib import Path
import numpy as np
from benchmark.workload.tasks.runner_task import RunnerTask
from benchmark.basic.my_logger import logger
from benchmark.workload.tasks.runner_type import RunnerType


class SearchTask(RunnerTask):
    """Task to execute search queries."""

    def __init__(self, name: str, config: Dict[str, Any], global_params: Dict[str, Any], workload_dir: Path):
        self.with_recall = False
        super().__init__(name, config, global_params, workload_dir)
        if config.get('recall'):
            self.with_recall = True
        self.runner.set_runner_type(RunnerType.SEARCH_WITH_RECALL if self.with_recall else RunnerType.SEARCH)

    def get_runner_type(self) -> RunnerType:
        if self.with_recall:
            return RunnerType.SEARCH_WITH_RECALL
        else:
            return RunnerType.INGEST

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

        if 'search_ids' in metrics and self.config.get('recall') is not None:
            search_ids = metrics.get('search_ids')
            recall_params = self.config.get('recall')
            recall = self._evaluate(search_ids, recall_params)
            logger.info("\n🎯 Search Recall:")
            logger.info(f"   {recall:.2f}")

        return metrics

    def _convert_search_ids(self, search_ids):
        """Convert search_ids dict to list of lists ordered by query_id."""
        if not search_ids:
            return []
        # Get the maximum query_id to determine the size of the result list
        max_query_id = max(int(query_id) for query_id in search_ids.keys())
        # Initialize result list with empty lists
        result = [[] for _ in range(max_query_id + 1)]
        
        # Fill the result list with document ids for each query_id
        for query_id, doc_ids in search_ids.items():
            result[int(query_id)] = doc_ids
        return result

    def _evaluate(self, search_ids, recall_params):
        size = recall_params.get('size')
        truth_file = recall_params.get('truth_file')
        if not os.path.exists(truth_file):
            logger.error(f"truth file: {truth_file} does not exist")
            return
        correct = np.loadtxt(truth_file, delimiter=",").astype(int)

        data = self._convert_search_ids(search_ids)
        # Use the minimum size to avoid out of bounds
        eval_size = min(len(data), len(correct))
        recalls = np.zeros(eval_size)
        for i in range(eval_size):
            data_set = set(int(x) for x in data[i])
            correct_set = set(int(x) for x in correct[i])
            recalls[i] = len(data_set & correct_set)
        ret = np.mean(recalls) / float(size)
        return ret

