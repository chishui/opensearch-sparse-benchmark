"""
LocustManager: Producer that manages a bounded queue and feeds bulk payloads
to LocustRunner consumers using true multiprocessing.
"""
import json
import time
import signal
import threading
from multiprocessing import Queue
from typing import Iterator, Tuple, Any
from benchmark.basic.my_logger import logger
from benchmark.locust.locust_runner import LocustRunner


class LocustManager:
    """
    Producer that manages bulk payload generation and queue feeding.

    Only puts data into the queue when it's not full (bounded queue).
    Uses multiprocessing for true parallel consumption.
    """

    def __init__(
        self,
        global_params: dict = None,
        queue_size: int = 100,
        bulk_size: int = 100,
        max_retries: int = 3,
        tag: str = ""
    ):
        """
        Initialize LocustManager.

        Args:
            global_params: Global parameters dict containing 'index', 'queue_size', 'bulk_size', etc.
            queue_size: Maximum size of the bounded queue (default, can be overridden by global_params)
            bulk_size: Number of documents per bulk request (default, can be overridden by global_params)
            max_retries: Maximum retry attempts for failed docs (default, can be overridden by global_params)
        """
        params = global_params or {}

        self.index_name = params.get('index')
        self.queue_size = int(params.get('queue_size', queue_size))
        self.bulk_size = int(params.get('bulk_size', bulk_size))
        self.max_retries = int(params.get('max_retries', max_retries))
        self.queue: Queue = Queue(maxsize=self.queue_size)
        self._total_produced = 0
        self._is_producing = False
        self._stop_requested = False
        self.runner: LocustRunner = None

        # Store original signal handler
        self._original_sigint = None
        self.tag = tag
        print(f"LocustManager - queue_size: {self.queue_size}, bulk_size: {self.bulk_size}, max retry: {self.max_retries}")

    def _create_bulk_body(self, docs: list[Tuple[Any, dict]]) -> str:
        """Create NDJSON bulk request body from documents."""
        lines = []
        for doc_id, doc_body in docs:
            action = {"index": {"_index": self.index_name, "_id": str(doc_id)}}
            lines.append(json.dumps(action))
            lines.append(json.dumps(doc_body))
        lines.append("")
        return '\n'.join(lines)

    def _handle_sigint(self, signum, frame):
        """Handle Ctrl+C by stopping all workers immediately."""
        print("\nReceived Ctrl+C, stopping workers...")
        self._stop_requested = True
        if self.runner:
            self.runner.stop(force=True)
        # Restore original handler
        signal.signal(signal.SIGINT, self._original_sigint)

    def _print_progress(self, current: int, total: int):
        """Print progress bar to terminal."""
        percent = current / total * 100
        bar_length = 40
        filled = int(bar_length * current / total)
        bar = '█' * filled + '░' * (bar_length - filled)
        print(f'\rProgress: [{bar}] {percent:.1f}% ({current}/{total})', end='', flush=True)

    def _produce_batch(self, data_generator: Iterator[Tuple[Any, dict]], block: bool = True, total_count: int = None):
        batch = []
        show_progress = total_count is not None and total_count > 0

        for doc_id, doc_body in data_generator:
            if self._stop_requested:
                break
            batch.append((doc_id, doc_body))

            if len(batch) >= self.bulk_size:
                payload = {
                    'body': self._create_bulk_body(batch),
                    'doc_count': len(batch),
                    'retry_count': 0
                }

                if block:
                    self.queue.put(payload)
                    self._total_produced += len(batch)
                else:
                    if not self.queue.full():
                        self.queue.put(payload)
                        self._total_produced += len(batch)

                if show_progress:
                    self._print_progress(self._total_produced, total_count)

                batch = []

        # Handle remaining docs
        if batch and len(batch) > 0 and not self._stop_requested:
            payload = {
                'body': self._create_bulk_body(batch),
                'doc_count': len(batch),
                'retry_count': 0
            }
            self.queue.put(payload, block=block)
            self._total_produced += len(batch)

            if show_progress:
                self._print_progress(self._total_produced, total_count)

        if show_progress:
            print()  # New line after progress bar

    def _produce_single(self, data_generator: Iterator[Tuple[Any, dict]], block: bool = True, total_count: int = None):
        show_progress = total_count is not None and total_count > 0

        for doc_id, doc_body in data_generator:
            if self._stop_requested:
                break

            payload = {
                'body': doc_body,
                'doc_count': 1,
                'retry_count': 0
            }

            self.queue.put(payload)
            self._total_produced += 1

            if show_progress:
                self._print_progress(self._total_produced, total_count)

        if show_progress:
            print()  # New line after progress bar


    def produce(self, data_generator: Iterator[Tuple[Any, dict]], block: bool = True, total_count: int = None):
        """
        Produce bulk payloads from data generator and put into queue.

        Args:
            data_generator: Iterator yielding (doc_id, doc_body) tuples
            block: If True, block when queue is full. If False, skip.
            total_count: Total number of documents (for progress bar). If None, no progress bar.
        """
        self._is_producing = True
        if self.tag == "ingest":
            self._produce_batch(data_generator, block, total_count)
        elif self.tag == "search":
            self._produce_single(data_generator, block, total_count)
        self._is_producing = False

    def run(
        self,
        data_generator: Iterator[Tuple[Any, dict]],
        user_count: int = 4,
        spawn_rate: int = 1,
        wait_for_completion: bool = True,
        total_count: int = None
    ) -> dict:
        """
        Run the load test with producer and consumers.

        Spawns all consumer processes first, then starts producing.

        Args:
            data_generator: Iterator yielding (doc_id, doc_body) tuples
            user_count: Number of worker processes
            spawn_rate: Ignored (kept for API compatibility)
            wait_for_completion: Wait for queue to drain before returning
            total_count: Total number of documents (for progress bar)

        Returns:
            Metrics dictionary with success/fail counts
        """
        # Setup signal handler for Ctrl+C
        self._original_sigint = signal.signal(signal.SIGINT, self._handle_sigint)
        self._stop_requested = False

        try:
            # Create and start runner with worker processes
            self.runner = LocustRunner(
                payload_queue=self.queue,
                index_name=self.index_name,
                max_retries=self.max_retries,
                num_workers=user_count,
                tag=self.tag
            )
            self.runner.start()

            # Wait for all workers to be ready
            if not self.runner.wait_until_ready(timeout=30.0):
                print(f"Warning: Not all workers started within timeout")
            print(f"All {self.runner.ready_count.value} consumers ready, starting to produce...")

            # Start producer in background thread
            producer_thread = threading.Thread(
                target=self.produce,
                args=(data_generator,),
                kwargs={'block': True, 'total_count': total_count}
            )
            producer_thread.start()

            # Wait for producer to finish
            producer_thread.join()
            
            if self._stop_requested:
                print("Stopped by user")
            else:
                print("Producer finished, waiting for queue to drain...")

                if wait_for_completion:
                    # Wait for queue to drain
                    while not self.queue.empty() and not self._stop_requested:
                        time.sleep(0.5)

                    # Give workers time to finish processing last items
                    if not self._stop_requested:
                        time.sleep(2)

            # Stop workers
            self.runner.stop()

            # Collect and aggregate metrics from all workers
            metrics = self.runner.collect_metrics()
            metrics['total_produced'] = self._total_produced

            return metrics
        finally:
            # Restore original signal handler
            signal.signal(signal.SIGINT, self._original_sigint)

    def stop(self):
        """Signal all runners to stop."""
        if self.runner:
            self.runner.stop()

    @property
    def total_produced(self) -> int:
        return self._total_produced

    @property
    def is_producing(self) -> bool:
        return self._is_producing

    @property
    def queue_size_current(self) -> int:
        return self.queue.qsize()

    @staticmethod
    def print_report(metrics: dict):
        """Print a detailed metrics report."""
        logger.info("\n" + "=" * 60)
        logger.info("LOAD TEST REPORT")
        logger.info("=" * 60)
        
        logger.info(f"\n📊 Overall Results:")
        logger.info(f"   Total Produced:  {metrics.get('total_produced', 0):,}")
        logger.info(f"   Total Processed: {metrics.get('total_docs', 0):,}")
        logger.info(f"   Success:         {metrics.get('total_success', 0):,}")
        logger.info(f"   Failed:          {metrics.get('total_fail', 0):,}")
        logger.info(f"   Success Rate:    {metrics.get('success_rate', 0):.2%}")
        
        logger.info(f"\n⚡ Performance:")
        logger.info(f"   Throughput:      {metrics.get('throughput', 0):,.2f} docs/sec")
        logger.info(f"   Total Requests:  {metrics.get('total_requests', 0):,}")
        logger.info(f"   Total Retries:   {metrics.get('total_retries', 0):,}")
        
        logger.info(f"\n⏱️ Latency (ms):")
        logger.info(f"   Average:         {metrics.get('avg_latency_ms', 0):.2f}")
        logger.info(f"   P50:             {metrics.get('p50_latency_ms', 0):.2f}")
        logger.info(f"   P95:             {metrics.get('p95_latency_ms', 0):.2f}")
        logger.info(f"   P99:             {metrics.get('p99_latency_ms', 0):.2f}")
        
        per_worker = metrics.get('per_worker', [])
        if per_worker:
            logger.info(f"\n👷 Per Worker ({len(per_worker)} workers):")
            for w in per_worker:
                logger.info(f"   Worker {w['runner_id']}: "
                      f"{w['success_count']:,} success, "
                      f"{w['fail_count']:,} fail, "
                      f"{w['throughput']:.2f} docs/sec, "
                      f"avg {w['avg_latency_ms']:.2f}ms")
        
        logger.info("\n" + "=" * 60)
