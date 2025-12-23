"""
LocustRunner: Multi-process consumer that processes bulk payloads from a Queue
and makes bulk requests to OpenSearch with retry logic.
Uses true multiprocessing for parallel execution.
"""
import time
import os
from dataclasses import dataclass, field
from multiprocessing import Process, Queue, Value
from queue import Empty
from ctypes import c_bool, c_int
from typing import Dict, Any, Optional, List
from opensearchpy import OpenSearch, RequestsHttpConnection
from opensearchpy.exceptions import OpenSearchException
from requests_aws4auth import AWS4Auth
from dotenv import load_dotenv
from benchmark.basic import client
from benchmark.basic.my_logger import logger, file_logger


@dataclass
class RunnerMetrics:
    """Metrics collected by each worker process."""
    runner_id: int
    success_count: int = 0
    fail_count: int = 0
    retry_count: int = 0
    request_count: int = 0
    total_docs: int = 0
    latencies: List[float] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    start_time: float = 0
    end_time: float = 0

    def to_dict(self) -> Dict[str, Any]:
        duration = self.end_time - self.start_time if self.end_time > self.start_time else 0
        latencies = sorted(self.latencies) if self.latencies else [0]
        return {
            "runner_id": self.runner_id,
            "success_count": self.success_count,
            "fail_count": self.fail_count,
            "retry_count": self.retry_count,
            "request_count": self.request_count,
            "total_docs": self.total_docs,
            "duration_sec": duration,
            "throughput": self.total_docs / duration if duration > 0 else 0,
            "avg_latency_ms": sum(latencies) / len(latencies) if latencies else 0,
            "min_latency_ms": min(latencies) if latencies else 0,
            "max_latency_ms": max(latencies) if latencies else 0,
            "p50_latency_ms": latencies[int(len(latencies) * 0.5)] if latencies else 0,
            "p95_latency_ms": latencies[int(len(latencies) * 0.95)] if len(latencies) > 1 else latencies[0] if latencies else 0,
            "p99_latency_ms": latencies[int(len(latencies) * 0.99)] if len(latencies) > 1 else latencies[0] if latencies else 0,
            "error_count": len(self.errors),
        }


def _extract_failed_docs(bulk_body: str, response: Dict) -> tuple[Optional[str], int]:
    """Extract failed documents from bulk response for retry."""
    if not response.get('errors'):
        return None, 0

    lines = bulk_body.strip().split('\n')
    failed_lines = []
    failed_count = 0

    for i, item in enumerate(response.get('items', [])):
        action = list(item.keys())[0]
        if item[action].get('error'):
            failed_count += 1
            action_idx = i * 2
            if action_idx + 1 < len(lines):
                failed_lines.append(lines[action_idx])
                failed_lines.append(lines[action_idx + 1])

    failed_body = '\n'.join(failed_lines) + '\n' if failed_lines else None
    return failed_body, failed_count


def _execute_bulk_with_retry(
    runner_id: int,
    index_name: str,
    bulk_body: str,
    doc_count: int,
    max_retries: int,
    metrics: RunnerMetrics
):
    """
    Execute bulk request with local retry logic.
    Retries are handled within this function, not by re-queuing.
    """
    current_body = bulk_body
    current_doc_count = doc_count
    retry_count = 0

    while current_body and retry_count <= max_retries:
        start_time = time.time()
        try:
            response = client.bulk(
                index=index_name,
                body=current_body,
                params={'refresh': 'false'}
            )
            elapsed_ms = (time.time() - start_time) * 1000
            metrics.latencies.append(elapsed_ms)
            metrics.request_count += 1
            metrics.total_docs += current_doc_count

            if response.get('errors'):
                file_logger.error(response)
                failed_body, failed_count = _extract_failed_docs(current_body, response)
                success_count = current_doc_count - failed_count
                metrics.success_count += success_count

                if failed_body and retry_count < max_retries:
                    # Retry locally with failed docs only
                    current_body = failed_body
                    current_doc_count = failed_count
                    retry_count += 1
                    metrics.retry_count += 1
                    file_logger.error(f'{runner_id} retrying {failed_count} failed docs (attempt {retry_count})')
                    continue
                else:
                    # Max retries reached or no failed body
                    metrics.fail_count += failed_count
                    break
            else:
                # All docs succeeded
                metrics.success_count += current_doc_count
                break

        except OpenSearchException as e:
            elapsed_ms = (time.time() - start_time) * 1000
            metrics.latencies.append(elapsed_ms)
            metrics.request_count += 1
            metrics.errors.append(str(e))

            if retry_count < max_retries:
                retry_count += 1
                metrics.retry_count += 1
                file_logger.error(f'{runner_id} retrying after exception (attempt {retry_count}): {e}')
                continue
            else:
                metrics.fail_count += current_doc_count
                break

def _execute_search(
    runner_id: int,
    index_name: str,
    query_body: str,
    metrics: RunnerMetrics
):
    """
    Execute a single search request with retry logic.
    """
    start_time = time.time()
    try:
        response = client.search(
            index=index_name,
            body=query_body
        )
        elapsed_ms = (time.time() - start_time) * 1000
        metrics.latencies.append(elapsed_ms)
        metrics.request_count += 1
        metrics.total_docs += 1
        metrics.success_count += 1
        return response

    except OpenSearchException as e:
        elapsed_ms = (time.time() - start_time) * 1000
        metrics.latencies.append(elapsed_ms)
        metrics.request_count += 1
        metrics.errors.append(str(e))
        metrics.fail_count += 1
        metrics.total_docs += 1
        return None


def _worker_process(
    runner_id: int,
    payload_queue: Queue,
    metrics_queue: Queue,
    index_name: str,
    max_retries: int,
    stop_signal: Value,
    ready_signal: Value,
    tag: str
):
    """
    Worker process that consumes bulk payloads and sends to OpenSearch.
    Aggregates metrics locally and sends to metrics_queue when done.
    Retries are handled locally - never puts data back into payload_queue.
    """
    # Signal that this worker is ready
    with ready_signal.get_lock():
        ready_signal.value += 1
    print(f"LocustRunner {runner_id} started")

    # Local metrics for this worker
    metrics = RunnerMetrics(runner_id=runner_id)
    metrics.start_time = time.time()

    while not stop_signal.value:
        try:
            payload = payload_queue.get(timeout=0.5)
        except Empty:
            time.sleep(1)
            continue

        bulk_body = payload.get('body', '')
        doc_count = payload.get('doc_count', 0)

        if tag == 'ingest':
            # Handle retries locally - no re-queuing
            _execute_bulk_with_retry(
                runner_id=runner_id,
                index_name=index_name,
                bulk_body=bulk_body,
                doc_count=doc_count,
                max_retries=max_retries,
                metrics=metrics
            )
        elif tag == 'search':
            response = _execute_search(
                runner_id=runner_id,
                index_name=index_name,
                query_body=bulk_body,
                metrics=metrics
            )


    # Worker done - send aggregated metrics to queue
    metrics.end_time = time.time()
    metrics_queue.put(metrics.to_dict())
    print(f"LocustRunner {runner_id} stopped")


class LocustRunner:
    """
    Multi-process runner that spawns worker processes to consume bulk payloads.
    Each worker runs in its own process for true parallelism.
    """

    def __init__(
        self,
        payload_queue: Queue,
        index_name: str = "test_index",
        max_retries: int = 3,
        num_workers: int = 4,
        tag: str = 'ingest'
    ):
        self.payload_queue = payload_queue
        self.index_name = index_name
        self.max_retries = max_retries
        self.num_workers = num_workers

        # Queue for receiving aggregated metrics from workers
        self.metrics_queue = Queue()
        self.stop_signal = Value(c_bool, False)
        self.ready_count = Value(c_int, 0)
        self.workers: list[Process] = []
        self.tag = tag

    def start(self):
        """Start all worker processes."""
        for i in range(self.num_workers):
            p = Process(
                target=_worker_process,
                args=(
                    i,
                    self.payload_queue,
                    self.metrics_queue,
                    self.index_name,
                    self.max_retries,
                    self.stop_signal,
                    self.ready_count,
                    self.tag
                )
            )
            p.start()
            self.workers.append(p)

    def wait_until_ready(self, timeout: float = 30.0) -> bool:
        """Wait until all workers are ready.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if all workers are ready, False if timeout
        """
        deadline = time.time() + timeout
        while self.ready_count.value < self.num_workers:
            if time.time() > deadline:
                return False
            time.sleep(0.1)
        return True

    def stop(self, force: bool = False):
        """Signal all workers to stop and wait for them to send metrics."""
        self.stop_signal.value = True

        if force:
            # Terminate immediately - no metrics will be collected
            for p in self.workers:
                if p.is_alive():
                    p.terminate()
        else:
            # Graceful shutdown - wait for workers to finish and send metrics
            for p in self.workers:
                p.join(timeout=5)  # Give more time for metrics to be sent
                if p.is_alive():
                    p.terminate()

        self.workers.clear()

    def collect_metrics(self, timeout: float = 2.0) -> Dict[str, Any]:
        """
        Collect and aggregate metrics from all workers.
        Call this after stop() to get final metrics.
        
        Args:
            timeout: Time to wait for metrics from each worker
        """
        worker_metrics = []

        # Wait for metrics from all workers
        expected_workers = self.num_workers
        deadline = time.time() + timeout
        
        while len(worker_metrics) < expected_workers and time.time() < deadline:
            try:
                metrics = self.metrics_queue.get(timeout=0.5)
                worker_metrics.append(metrics)
            except Empty:
                continue

        if not worker_metrics:
            return {
                "total_success": 0,
                "total_fail": 0,
                "total_docs": 0,
                "success_rate": 0,
                "worker_count": self.num_workers,
                "per_worker": []
            }

        # Aggregate metrics
        total_success = sum(m['success_count'] for m in worker_metrics)
        total_fail = sum(m['fail_count'] for m in worker_metrics)
        total_docs = sum(m['total_docs'] for m in worker_metrics)
        total_requests = sum(m['request_count'] for m in worker_metrics)
        total_retries = sum(m['retry_count'] for m in worker_metrics)
        total_errors = sum(m['error_count'] for m in worker_metrics)
        
        # Aggregate latencies from all workers
        all_latencies = []
        for m in worker_metrics:
            # Reconstruct latencies from percentiles (approximate)
            if m['avg_latency_ms'] > 0:
                all_latencies.extend([m['avg_latency_ms']] * m['request_count'])

        all_latencies = sorted(all_latencies) if all_latencies else [0]

        # Calculate overall throughput
        total_throughput = sum(m['throughput'] for m in worker_metrics)

        return {
            "total_success": total_success,
            "total_fail": total_fail,
            "total_docs": total_docs,
            "total_requests": total_requests,
            "total_retries": total_retries,
            "total_errors": total_errors,
            "success_rate": total_success / (total_success + total_fail) if (total_success + total_fail) > 0 else 0,
            "throughput": total_throughput,
            "avg_latency_ms": sum(all_latencies) / len(all_latencies) if all_latencies else 0,
            "p50_latency_ms": all_latencies[int(len(all_latencies) * 0.5)] if all_latencies else 0,
            "p95_latency_ms": all_latencies[int(len(all_latencies) * 0.95)] if len(all_latencies) > 1 else all_latencies[0],
            "p99_latency_ms": all_latencies[int(len(all_latencies) * 0.99)] if len(all_latencies) > 1 else all_latencies[0],
            "worker_count": len(worker_metrics),
            "per_worker": worker_metrics
        }

    @property
    def user_count(self) -> int:
        """Number of active workers."""
        return len([p for p in self.workers if p.is_alive()])
