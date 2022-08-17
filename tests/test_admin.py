import unittest
from unittest.mock import Mock

from mysql_mimic import Session
from mysql_mimic.charset import CharacterSet
from mysql_mimic.results import ResultSet
from mysql_mimic.admin import Admin


class TestAdmin(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # pylint: disable=attribute-defined-outside-init
        self.session = Mock(spec=Session)
        self.session.show_columns.return_value = [
            {"name": "col1", "type": "INTEGER"},
            {"name": "col2", "type": "INTEGER"},
        ]
        self.cmd = Admin(connection_id=1, session=self.session)
        self.cmd.database = "db"

    async def test_parse_show_columns(self):
        for cmd, expected, result_length, col_length in [
            ("show columns from table", ("db", "table"), 2, 6),
            (
                " SHOW  EXTENDED  FULL  FIELDS  IN  `table`  IN  `db2` ",
                ("db2", "table"),
                2,
                9,
            ),
            ("show columns from table like '%2'", ("db", "table"), 1, 6),
            ("show columns from table like 'col1'", ("db", "table"), 1, 6),
            ("show columns from table like 'col%'", ("db", "table"), 2, 6),
            ("show columns from table like '%col%'", ("db", "table"), 2, 6),
            ("show columns from table like '_ol2'", ("db", "table"), 1, 6),
        ]:
            result = await self.cmd.parse(cmd)
            self.session.show_columns.assert_called_with(*expected)
            self.assertIsInstance(result, ResultSet)
            self.assertEqual(len(list(result.rows)), result_length)
            self.assertEqual(len(result.columns), col_length)

    async def test_parse_show_index(self):
        for cmd, result_length, col_length in [
            ("show index from table", 0, 15),
        ]:
            result = await self.cmd.parse(cmd)
            self.assertIsInstance(result, ResultSet)
            self.assertEqual(len(list(result.rows)), result_length)
            self.assertEqual(len(result.columns), col_length)

    async def test_parse_show_variables(self):
        for cmd, expected in [
            (
                "show variables",
                {
                    "version_comment": "mysql-mimic",
                    "version": "8.0.29",
                    "character_set_client": "utf8mb4",
                    "character_set_results": "utf8mb4",
                    "character_set_server": "utf8mb4",
                    "collation_server": "utf8mb4_general_ci",
                    "collation_database": "utf8mb4_general_ci",
                },
            ),
            (
                "SHOW  SESSION  VARIABLES  LIKE 'version_comment'",
                {"version_comment": "mysql-mimic"},
            ),
            (
                "SHOW  SESSION  VARIABLES  LIKE 'version_%'",
                {"version_comment": "mysql-mimic"},
            ),
            ("SET @@version_comment = 'hello'", {}),
            (
                "SHOW  SESSION  VARIABLES  LIKE 'version_comment'",
                {"version_comment": "hello"},
            ),
        ]:
            result = await self.cmd.parse(cmd)
            self.assertIsInstance(result, ResultSet)
            rows = {r[0]: r[1] for r in result.rows}
            self.assertEqual(rows, expected)

    async def test_parse_set_names(self):
        for cmd, expected_charset, expected_collation in [
            (
                "SET NAMES utf8",
                "utf8",
                "utf8_general_ci",
            ),
            (
                "SET NAMES big5 DEFAULT",
                "big5",
                "big5_chinese_ci",
            ),
            (
                "SET NAMES utf8 COLLATE utf8mb4_bin",
                "utf8",
                "utf8mb4_bin",
            ),
            (
                "set  names  'big5'  collate  'big5_chinese_ci'",
                "big5",
                "big5_chinese_ci",
            ),
        ]:
            await self.cmd.parse(cmd)
            self.assertEqual(
                self.cmd.variables["character_set_client"], expected_charset
            )
            self.assertEqual(
                self.cmd.variables["character_set_connection"], expected_charset
            )
            self.assertEqual(
                self.cmd.variables["character_set_results"], expected_charset
            )
            self.assertEqual(
                self.cmd.variables["collation_connection"], expected_collation
            )
            self.assertEqual(
                self.cmd.client_character_set, CharacterSet[expected_charset]
            )
            self.assertEqual(
                self.cmd.server_character_set, CharacterSet[expected_charset]
            )

    async def test_parse_set_variables(self):
        for cmd, expected in [
            ("SET version_comment = 'b'", "b"),
            ("SET @@version_comment = ON", True),
            ("SET  @@SESSION.version_comment  = OFF", False),
            ("set version_comment = DEFAULT", "mysql-mimic"),
            ("set version_comment = 1", 1),
            ("set version_comment = 2.3", 2.3),
            ("set version_comment = NULL", "mysql-mimic"),
        ]:
            await self.cmd.parse(cmd)
            self.assertEqual(self.cmd.variables["version_comment"], expected)
