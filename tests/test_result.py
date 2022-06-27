import unittest

from mysql_mimic.errors import MysqlError
from mysql_mimic.results import ensure_result_set


class TestResult(unittest.TestCase):
    def test_ensure_result_set__invalid(self):
        for result in [
            [1, 2],
            ([[1, 2]], ["a", "b"], ["a", "b"]),
            ([[1, 2]], [1, 2]),
        ]:
            with self.assertRaises(MysqlError):
                ensure_result_set(result)
