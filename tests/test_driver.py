import os.path
import unittest
import uuid

from loguru import logger

from benchmarks.gray_sort_benchmark import gray_sort_benchmark
from examples.sort_mock_urls import sort_mock_urls
from smallpond.common import GB, MB
from smallpond.execution.driver import Driver
from tests.test_fabric import TestFabric


@unittest.skipUnless(os.getenv("ENABLE_DRIVER_TEST"), "unit test disabled")
class TestDriver(TestFabric, unittest.TestCase):

    fault_inject_prob = 0.05

    def create_driver(self, num_executors: int):
        cmdline = f"scheduler --job_id {str(uuid.uuid4())} --job_name {self._testMethodName} --data_root {self.output_root_abspath} --num_executors {num_executors} --fault_inject_prob {self.fault_inject_prob}"
        driver = Driver()
        driver.parse_arguments(args=cmdline.split())
        logger.info(f"{cmdline=} {driver.mode=} {driver.job_id=} {driver.data_root=}")
        return driver

    def test_standalone_mode(self):
        plan = sort_mock_urls(["tests/data/mock_urls/*.tsv"], npartitions=3)
        driver = self.create_driver(num_executors=0)
        exec_plan = driver.run(plan, stop_process_on_done=False)
        self.assertTrue(exec_plan.successful)
        self.assertGreater(exec_plan.final_output.num_files, 0)

    def test_run_on_remote_executors(self):
        driver = self.create_driver(num_executors=2)
        plan = gray_sort_benchmark(
            record_nbytes=100,
            key_nbytes=10,
            total_data_nbytes=1 * GB,
            gensort_batch_nbytes=100 * MB,
            num_data_partitions=10,
            num_sort_partitions=10,
            validate_results=True,
        )
        exec_plan = driver.run(plan, stop_process_on_done=False)
        self.assertTrue(exec_plan.successful)
        self.assertGreater(exec_plan.final_output.num_files, 0)
