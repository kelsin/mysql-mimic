import unittest

from mysql_mimic.result import ensure_result_set


class TestResult(unittest.TestCase):
    def test_ensure_result_set__invalid(self):
        for result in [
            [1, 2],
            ([[1, 2]], ["a", "b"], ["a", "b"]),
            ([[1, 2]], [1, 2]),
        ]:
            with self.assertRaises(ValueError):
                ensure_result_set(result)
