import unittest

from mysql_mimic.variables import parse_set_command


class TestVariables(unittest.TestCase):
    def test_parse_set_command(self):
        for cmd, expected, defaults in [
            (
                "SET NAMES utf8",
                {
                    "character_set_client": "utf8",
                    "character_set_connection": "utf8",
                    "character_set_results": "utf8",
                    "collation_connection": "utf8_general_ci",
                },
                None,
            ),
            (
                "SET NAMES utf8 DEFAULT",
                {
                    "character_set_client": "utf8",
                    "character_set_connection": "utf8",
                    "character_set_results": "utf8",
                    "collation_connection": "utf8_general_ci",
                },
                None,
            ),
            (
                "SET NAMES utf8 COLLATE utf8mb4_general_ci",
                {
                    "character_set_client": "utf8",
                    "character_set_connection": "utf8",
                    "character_set_results": "utf8",
                    "collation_connection": "utf8mb4_general_ci",
                },
                None,
            ),
            (
                "set  names  'utf8'  collate  'utf8mb4_general_ci'",
                {
                    "character_set_client": "utf8",
                    "character_set_connection": "utf8",
                    "character_set_results": "utf8",
                    "collation_connection": "utf8mb4_general_ci",
                },
                None,
            ),
            (
                "set character_set_results = NULL",
                {
                    "character_set_results": None,
                },
                None,
            ),
            (
                "set @@character_set_results = OFF",
                {
                    "character_set_results": False,
                },
                None,
            ),
        ]:
            self.assertEqual(parse_set_command(cmd, defaults), expected)
