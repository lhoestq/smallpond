import random
import subprocess
import time
import unittest
from typing import Iterable

from smallpond.utility import ConcurrentIter, execute_command
from tests.test_fabric import TestFabric


class TestUtility(TestFabric, unittest.TestCase):
    def test_concurrent_iter_no_error(self):
        def slow_iterator(iter: Iterable[int], sleep_ms: int):
            for i in iter:
                time.sleep(sleep_ms / 1000)
                yield i

        for n in [1, 5, 10, 50, 100]:
            with ConcurrentIter(slow_iterator(range(n), 2)) as iter1:
                with ConcurrentIter(slow_iterator(iter1, 5)) as iter2:
                    self.assertEqual(sum(slow_iterator(iter2, 1)), sum(range(n)))

    def test_concurrent_iter_with_error(self):
        def broken_iterator(iter: Iterable[int], sleep_ms: int):
            for i in iter:
                time.sleep(sleep_ms / 1000)
                if random.randint(1, 10) == 1:
                    raise Exception("raised before yield")
                yield i
                if random.randint(1, 10) == 1:
                    raise Exception("raised after yield")
            raise Exception("raised at the end")

        for n in [1, 5, 10, 50, 100]:
            with self.assertRaises(Exception):
                with ConcurrentIter(range(n)) as iter:
                    print(sum(broken_iterator(iter, 1)))
            with self.assertRaises(Exception):
                with ConcurrentIter(broken_iterator(range(n), 2)) as iter1:
                    with ConcurrentIter(broken_iterator(iter1, 5)) as iter2:
                        print(sum(iter2))

    def test_execute_command(self):
        with self.assertRaises(subprocess.CalledProcessError):
            for line in execute_command("ls non_existent_file"):
                print(line)
        for line in execute_command("echo hello"):
            print(line)
        for line in execute_command("cat /dev/null"):
            print(line)
