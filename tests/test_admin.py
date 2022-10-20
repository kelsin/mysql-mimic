from typing import Dict, Any

import pytest

from mysql_mimic.charset import CharacterSet
from mysql_mimic.results import ResultSet
from mysql_mimic.admin import Admin
from mysql_mimic.variables import SystemVariables
from tests.conftest import MockSession


@pytest.fixture(scope="module")
def session() -> MockSession:
    return MockSession()


@pytest.fixture(scope="module")
def admin(session: MockSession) -> Admin:
    session.columns = {
        ("db", "table"): [
            {"name": "col1", "type": "INTEGER"},
            {"name": "col2", "type": "INTEGER"},
        ]
    }
    admin = Admin(connection_id=1, session=session, variables=SystemVariables())
    admin.database = "db"
    return admin


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cmd,result_length,col_length",
    [
        ("show columns from table", 2, 6),
        (
            " SHOW  EXTENDED  FULL  FIELDS  IN  `table`  IN  `db` ",
            2,
            9,
        ),
        ("show columns from table like '%2'", 1, 6),
        ("show columns from table like 'col1'", 1, 6),
        ("show columns from table like 'col%'", 2, 6),
        ("show columns from table like '%col%'", 2, 6),
        ("show columns from table like '_ol2'", 1, 6),
        ("show tables from db", 1, 1),
        ("show full tables from db", 1, 2),
        ("show tables from db like 'table'", 1, 1),
        ("show tables from db like 'x'", 0, 1),
        ("show databases", 1, 1),
        ("show schemas", 1, 1),
        ("show databases like 'db'", 1, 1),
        ("show databases like 'x'", 0, 1),
    ],
)
async def test_parse_show(
    session: MockSession,
    admin: Admin,
    cmd: str,
    result_length: int,
    col_length: int,
) -> None:
    result = await admin.parse(cmd)
    assert isinstance(result, ResultSet)
    assert len(list(result.rows)) == result_length
    assert len(result.columns) == col_length


@pytest.mark.asyncio
async def test_parse_show_index(admin: Admin) -> None:
    cmd = "show index from table"
    result_length = 0
    col_length = 15
    result = await admin.parse(cmd)
    assert isinstance(result, ResultSet)
    assert len(list(result.rows)) == result_length
    assert len(result.columns) == col_length


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cmd,expected",
    [
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
                "lower_case_table_names": 0,
                "sql_mode": "",
                "transaction_isolation": "READ-COMMITTED",
                "external_user": "",
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
    ],
)
async def test_parse_show_variables(
    admin: Admin, cmd: str, expected: Dict[str, Any]
) -> None:
    result = await admin.parse(cmd)
    assert isinstance(result, ResultSet)
    rows = {r[0]: r[1] for r in result.rows}
    assert rows == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cmd,expected_charset,expected_collation",
    [
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
    ],
)
async def test_parse_set_names(
    admin: Admin, cmd: str, expected_charset: str, expected_collation: str
) -> None:
    await admin.parse(cmd)
    assert admin.vars["character_set_client"] == expected_charset
    assert admin.vars["character_set_connection"] == expected_charset
    assert admin.vars["character_set_results"] == expected_charset
    assert admin.vars["collation_connection"] == expected_collation
    assert admin.vars.client_charset == CharacterSet[expected_charset]
    assert admin.vars.server_charset == CharacterSet[expected_charset]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cmd,expected",
    [
        ("SET version_comment = 'b'", "b"),
        ("SET @@version_comment = ON", True),
        ("SET  @@SESSION.version_comment  = OFF", False),
        ("set version_comment = DEFAULT", "mysql-mimic"),
        ("set version_comment = 1", 1),
        ("set version_comment = 2.3", 2.3),
        ("set version_comment = NULL", "mysql-mimic"),
    ],
)
async def test_parse_set_variables(admin: Admin, cmd: str, expected: str) -> None:
    await admin.parse(cmd)
    assert admin.vars["version_comment"] == expected
