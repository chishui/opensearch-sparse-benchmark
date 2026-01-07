from typing import Any, Dict
from pathlib import Path
from benchmark.workload.task import Task
from benchmark.locust.locust_manager import LocustManager
from benchmark.basic.my_logger import logger
from benchmark.workload.tasks.runner_type import RunnerType


class RunnerTask(Task):
    def __init__(self, name: str, config: Dict[str, Any], global_params: Dict[str, Any], workload_dir: Path):
        super().__init__(name, config, global_params, workload_dir)
        self.runner = LocustManager(global_params, runner_type=self.get_runner_type())
        
    def get_runner_type(self) -> RunnerType:
        return RunnerType.UNKNOWN

    def print_report(self, metrics: Dict[str, Any]) -> None:
        """Print task metrics report with query timing."""
        logger.info("\n" + "=" * 60)
        logger.info(f"{self.name} LOAD TEST REPORT")
        logger.info("=" * 60)

        logger.info("\n📊 Overall Results:")
        logger.info("   Total Produced:  %s", f"{metrics.get('total_produced', 0):,}")
        logger.info("   Total Processed: %s", f"{metrics.get('total_docs', 0):,}")
        logger.info("   Success:         %s", f"{metrics.get('total_success', 0):,}")
        logger.info("   Failed:          %s", f"{metrics.get('total_fail', 0):,}")
        logger.info("   Success Rate:    %s", f"{metrics.get('success_rate', 0):.2%}")

        logger.info("\n⚡ Performance:")
        logger.info("   Throughput:      %s docs/sec", f"{metrics.get('throughput', 0):,.2f}")
        logger.info("   Total Requests:  %s", f"{metrics.get('total_requests', 0):,}")
        logger.info("   Total Retries:   %s", f"{metrics.get('total_retries', 0):,}")

        logger.info("\n⏱️ Latency (ms):")
        logger.info("   Average:         %s", f"{metrics.get('avg_latency_ms', 0):.2f}")
        logger.info("   P50:             %s", f"{metrics.get('p50_latency_ms', 0):.2f}")
        logger.info("   P95:             %s", f"{metrics.get('p95_latency_ms', 0):.2f}")
        logger.info("   P99:             %s", f"{metrics.get('p99_latency_ms', 0):.2f}")

        # Search-specific: OpenSearch "took" metrics
        if 'avg_took_ms' in metrics:
            logger.info("\n🔍 OpenSearch Query Time - took (ms):")
            logger.info("   Average:         %s", f"{metrics.get('avg_took_ms', 0):.2f}")
            logger.info("   P50:             %s", f"{metrics.get('p50_took_ms', 0):.2f}")
            logger.info("   P95:             %s", f"{metrics.get('p95_took_ms', 0):.2f}")
            logger.info("   P99:             %s", f"{metrics.get('p99_took_ms', 0):.2f}")

        self._print_per_worker(metrics)
        logger.info("\n" + "=" * 60)

    def _print_per_worker(self, metrics: Dict[str, Any]) -> None:
        """Print per-worker statistics."""
        per_worker = metrics.get('per_worker', [])
        if per_worker:
            logger.info("\n👷 Per Worker (%d workers):", len(per_worker))
            for w in per_worker:
                logger.info(
                    "   Worker %s: %s success, %s fail, %s docs/sec, avg %sms",
                    w['runner_id'],
                    f"{w['success_count']:,}",
                    f"{w['fail_count']:,}",
                    f"{w['throughput']:.2f}",
                    f"{w['avg_latency_ms']:.2f}"
                )
