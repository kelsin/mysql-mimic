import io
from contextlib import closing
from datetime import date, datetime, timedelta
from functools import partial
from typing import Any, Callable, Awaitable, Sequence, Dict, List, Tuple

import pytest
import pytest_asyncio
import sqlalchemy.engine
from mysql.connector import MySQLConnection
from sqlalchemy import text
import aiomysql

from mysql_mimic import ResultColumn, ResultSet, MysqlServer
from mysql_mimic.charset import CharacterSet
from mysql_mimic.results import AllowedResult
from mysql_mimic.types import ColumnType
from tests.conftest import PreparedDictCursor, query, MockSession, ConnectFixture

QueryFixture = Callable[[str], Awaitable[Sequence[Dict[str, Any]]]]


@pytest_asyncio.fixture(
    params=["mysql.connector", "mysql.connector(prepared)", "aiomysql", "sqlalchemy"]
)
async def query_fixture(
    mysql_connector_conn: MySQLConnection,
    aiomysql_conn: aiomysql.Connection,
    session: MockSession,
    sqlalchemy_engine: sqlalchemy.engine.Engine,
    request: Any,
) -> QueryFixture:
    if request.param == "mysql.connector":

        async def q1(sql: str) -> Sequence[Dict[str, Any]]:
            return await query(mysql_connector_conn, sql)

        return q1

    if request.param == "mysql.connector(prepared)":

        async def q2(sql: str) -> Sequence[Dict[str, Any]]:
            return await query(
                mysql_connector_conn, sql, cursor_class=PreparedDictCursor
            )

        return q2

    if request.param == "aiomysql":

        async def q3(sql: str) -> Sequence[Dict[str, Any]]:
            async with aiomysql_conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql)
                return await cur.fetchall()

        return q3

    if request.param == "sqlalchemy":

        async def q4(sql: str) -> Sequence[Dict[str, Any]]:
            async with sqlalchemy_engine.connect() as conn:
                cursor = await conn.execute(text(sql))
                if cursor.returns_rows:
                    return cursor.mappings().all()
                return []

        return q4

    raise Exception("Unexpected fixture param")


# # Uncomment to make tests only use mysql-connector, which can help during debugging
# @pytest_asyncio.fixture
# async def query_fixture(
#     mysql_connector_conn: MySQLConnection,
# ) -> QueryFixture:
#     async def q1(sql: str) -> Sequence[Dict[str, Any]]:
#         return await query(mysql_connector_conn, sql)
#
#     return q1


EXPLICIT_TYPE_TESTS = [
    (
        ResultSet(
            rows=[(input_,)],
            columns=[
                ResultColumn(
                    name="b",
                    character_set=CharacterSet.utf8mb4,
                    type=type_,
                )
            ],
        ),
        [{"b": output}],
    )
    for input_, type_, output in [
        ("♥".encode("utf-8"), ColumnType.VARCHAR, "♥"),
        ("♥".encode("utf-8"), ColumnType.BLOB, "♥"),
        (b"\xe2\x99\xa5", ColumnType.BLOB, "♥"),
        (1, ColumnType.TINY, True),
        (2, ColumnType.SHORT, 2),
        (2, ColumnType.INT24, 2),
        (2, ColumnType.LONG, 2),
        (2, ColumnType.LONGLONG, 2),
        (1.0, ColumnType.FLOAT, 1.0),
        (1.0, ColumnType.DOUBLE, 1.0),
    ]
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rv, expected",
    [
        (((("x",),), ("b",)), [{"b": "x"}]),
        (([["1"]], ["b"]), [{"b": "1"}]),
        (([(1,)], ("b",)), [{"b": 1}]),
        (([(1.0,)], ("b",)), [{"b": 1.0}]),
        (([(b"x",)], ("b",)), [{"b": "x"}]),
        (([(True,)], ("b",)), [{"b": True}]),
        (([(date(2021, 1, 1),)], ("b",)), [{"b": date(2021, 1, 1)}]),
        (
            ([(datetime(2021, 1, 1, 1, 1, 1),)], ("b",)),
            [{"b": datetime(2021, 1, 1, 1, 1, 1)}],
        ),
        (([(timedelta(seconds=60),)], ("b",)), [{"b": timedelta(seconds=60)}]),
        (([(None,)], ["b"]), [{"b": None}]),
        (([(None, 1, 1)], ["a", "b", "c"]), [{"a": None, "b": 1, "c": 1}]),
        (([(1, None, 1)], ["a", "b", "c"]), [{"a": 1, "b": None, "c": 1}]),
        (([(1, 1, None)], ["a", "b", "c"]), [{"a": 1, "b": 1, "c": None}]),
        (([[None], [1]], ["b"]), [{"b": None}, {"b": 1}]),
        (
            ResultSet(
                rows=[("hello",)],
                columns=[
                    ResultColumn(
                        name="b",
                        character_set=CharacterSet.utf8mb4,
                        type=ColumnType.VARCHAR,
                        text_encoder=lambda col, val: b"world",
                        binary_encoder=lambda col, val: b"\x05world",
                    )
                ],
            ),
            [{"b": "world"}],
        ),
        *EXPLICIT_TYPE_TESTS,
    ],
)
async def test_query(
    session: MockSession,
    server: MysqlServer,
    rv: AllowedResult,
    expected: List[Dict[str, Any]],
    query_fixture: QueryFixture,
) -> None:
    session.return_value = rv
    result = await query_fixture("SELECT b FROM a")
    assert expected == result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sql, params, expected",
    [
        ("SELECT ? FROM x", ("1",), "SELECT '1' FROM x"),
        ("SELECT '?' FROM x", (), "SELECT '?' FROM x"),
        ("SELECT ? FROM x", (1,), "SELECT 1 FROM x"),
        ("SELECT ? FROM x", (0,), "SELECT 0 FROM x"),
        ("SELECT ? FROM x", (-1,), "SELECT -1 FROM x"),
        ("SELECT ? FROM x", (255,), "SELECT 255 FROM x"),
        ("SELECT ? FROM x", (-128,), "SELECT -128 FROM x"),
        ("SELECT ? FROM x", (65535,), "SELECT 65535 FROM x"),
        ("SELECT ? FROM x", (-32767,), "SELECT -32767 FROM x"),
        ("SELECT ? FROM x", (4294967295,), "SELECT 4294967295 FROM x"),
        ("SELECT ? FROM x", (-2147483648,), "SELECT -2147483648 FROM x"),
        (
            "SELECT ? FROM x",
            (18446744073709551615,),
            "SELECT 18446744073709551615 FROM x",
        ),
        (
            "SELECT ? FROM x",
            (-9223372036854775808,),
            "SELECT -9223372036854775808 FROM x",
        ),
        ("SELECT ? FROM x", (1.1,), "SELECT 1.1 FROM x"),
        ("SELECT ? FROM x", (1.7e308,), "SELECT 1.7e+308 FROM x"),
        ("SELECT ? FROM x", (None,), "SELECT NULL FROM x"),
        ("SELECT ? FROM x", (b"hello",), "SELECT 'hello' FROM x"),
        ("SELECT ? FROM x", (io.BytesIO(b"hello"),), "SELECT 'hello' FROM x"),
        ("SELECT ?, ? FROM x", ("1", "1"), "SELECT '1', '1' FROM x"),
        (
            "SELECT ?, ?, ?, ? FROM x",
            ("1", None, io.BytesIO(b"hello"), 1),
            "SELECT '1', NULL, 'hello', 1 FROM x",
        ),
    ],
)
async def test_prepared_stmt(
    session: MockSession,
    server: MysqlServer,
    mysql_connector_conn: MySQLConnection,
    sql: str,
    params: Tuple[Any],
    expected: str,
) -> None:
    session.echo = True
    result = await query(
        conn=mysql_connector_conn,
        sql=sql,
        cursor_class=PreparedDictCursor,
        params=params,
    )
    assert expected == result[0]["sql"]


@pytest.mark.asyncio
async def test_init(port: int, session: MockSession, server: MysqlServer) -> None:
    async with aiomysql.connect(
        port=port, user="levon_helm", db="db", program_name="test"
    ):
        connection = session.connection
        assert connection is not None
        assert connection.username == "levon_helm"
        assert connection.client_connect_attrs["program_name"] == "test"
        assert connection.database == "db"
        connection.username = "robbie_robertson"
        assert connection.username == "robbie_robertson"


@pytest.mark.asyncio
async def test_connection_id(port: int, server: MysqlServer) -> None:
    async with aiomysql.connect(port=port) as conn1:
        async with aiomysql.connect(port=port) as conn2:
            assert conn1.server_thread_id[0] + 1 == conn2.server_thread_id[0]


@pytest.mark.asyncio
async def test_replace_function(
    session: MockSession, server: MysqlServer, connect: ConnectFixture
) -> None:
    session.echo = True

    with closing(await connect(user="levon_helm", database="db")) as conn:
        result = await query(conn, "SELECT CONNECTION_ID()")
        assert result[0]["CONNECTION_ID()"] is not None

        result = await query(conn, "SELECT CURRENT_USER")
        assert result[0]["CURRENT_USER"] == "levon_helm"

        result = await query(conn, "SELECT USER()")
        assert result[0]["USER()"] == "levon_helm"

        result = await query(conn, "SELECT DATABASE()")
        assert result[0]["DATABASE()"] == "db"


@pytest.mark.asyncio
async def test_query_attributes(
    session: MockSession, server: MysqlServer, mysql_connector_conn: MySQLConnection
) -> None:
    session.echo = True

    for i, q in enumerate(
        [
            partial(query, conn=mysql_connector_conn),
            partial(query, conn=mysql_connector_conn, cursor_class=PreparedDictCursor),
        ]
    ):
        session.last_query_attrs = None
        sql = "SELECT 1 FROM x"
        query_attrs = {
            "idx": i,
            "str": "foo",
            "int": 1,
            "float": 1.1,
        }
        await q(sql=sql, query_attributes=query_attrs)
        assert session.last_query_attrs == query_attrs


# pylint: disable=trailing-whitespace
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sql, expected",
    [
        # SET session variable
        (
            "SET SESSION sql_mode = 'TRADITIONAL'; SELECT @@sql_mode",
            [{"@@sql_mode": "TRADITIONAL"}],
        ),
        (
            "SET LOCAL sql_mode = 'TRADITIONAL'; SELECT @@sql_mode",
            [{"@@sql_mode": "TRADITIONAL"}],
        ),
        (
            "SET @@SESSION.sql_mode = 'TRADITIONAL'; SELECT @@sql_mode",
            [{"@@sql_mode": "TRADITIONAL"}],
        ),
        (
            "SET @@LOCAL.sql_mode = 'TRADITIONAL'; SELECT @@sql_mode",
            [{"@@sql_mode": "TRADITIONAL"}],
        ),
        (
            "SET @@sql_mode = 'TRADITIONAL'; SELECT @@sql_mode",
            [{"@@sql_mode": "TRADITIONAL"}],
        ),
        (
            "SET sql_mode = 'TRADITIONAL'; SELECT @@sql_mode",
            [{"@@sql_mode": "TRADITIONAL"}],
        ),
        (
            "select @@version_comment limit 1",
            [{"@@version_comment": "mysql-mimic"}],
        ),
        # SET referencing other parameters
        (
            """
            SET character_set_connection = 'big5';
            SET character_set_client = @@character_set_connection; SELECT @@character_set_client""",
            [{"@@character_set_client": "big5"}],
        ),
        # SET multiple variables at once
        (
            "SET autocommit = OFF, sql_mode = 'TRADITIONAL'; SELECT @@autocommit, @@sql_mode",
            [{"@@autocommit": False, "@@sql_mode": "TRADITIONAL"}],
        ),
        ## SET names
        (
            """
            SET NAMES utf8;
            SELECT @@character_set_client, @@SESSION.character_set_connection, @@character_set_results, @@collation_connection""",
            [
                {
                    "@@character_set_client": "utf8",
                    "@@SESSION.character_set_connection": "utf8",
                    "@@character_set_results": "utf8",
                    "@@collation_connection": "utf8_general_ci",
                }
            ],
        ),
        (
            """
            SET NAMES big5; SET NAMES DEFAULT;
            SELECT @@character_set_client, @@SESSION.character_set_connection, @@character_set_results, @@collation_connection""",
            [
                {
                    "@@character_set_client": "utf8mb4",
                    "@@SESSION.character_set_connection": "utf8mb4",
                    "@@character_set_results": "utf8mb4",
                    "@@collation_connection": "utf8mb4_general_ci",
                }
            ],
        ),
        (
            """
            SET NAMES utf8 COLLATE utf8mb4_bin;
            SELECT @@character_set_client, @@SESSION.character_set_connection, @@character_set_results, @@collation_connection""",
            [
                {
                    "@@character_set_client": "utf8",
                    "@@SESSION.character_set_connection": "utf8",
                    "@@character_set_results": "utf8",
                    "@@collation_connection": "utf8mb4_bin",
                }
            ],
        ),
        (
            """
            set names 'big5' collate 'big5_chinese_ci';
            SELECT @@character_set_client, @@SESSION.character_set_connection, @@character_set_results, @@collation_connection""",
            [
                {
                    "@@character_set_client": "big5",
                    "@@SESSION.character_set_connection": "big5",
                    "@@character_set_results": "big5",
                    "@@collation_connection": "big5_chinese_ci",
                }
            ],
        ),
        # SET character set
        (
            """
            SET CHARSET 'utf8';
            SELECT @@character_set_client, @@SESSION.character_set_connection, @@character_set_results""",
            [
                {
                    "@@character_set_client": "utf8",
                    "@@SESSION.character_set_connection": "utf8mb4",
                    "@@character_set_results": "utf8",
                }
            ],
        ),
        (
            """
            SET CHARSET utf8;
            SELECT @@character_set_client, @@SESSION.character_set_connection, @@character_set_results""",
            [
                {
                    "@@character_set_client": "utf8",
                    "@@SESSION.character_set_connection": "utf8mb4",
                    "@@character_set_results": "utf8",
                }
            ],
        ),
        (
            """
            SET CHARSET utf8; SET CHARSET DEFAULT;
            SELECT @@character_set_client, @@SESSION.character_set_connection, @@character_set_results""",
            [
                {
                    "@@character_set_client": "utf8mb4",
                    "@@SESSION.character_set_connection": "utf8mb4",
                    "@@character_set_results": "utf8mb4",
                }
            ],
        ),
        # Types
        ("SET autocommit = OFF; SELECT @@autocommit AS x", [{"x": False}]),
        ("SET autocommit = ON; SELECT @@autocommit AS x", [{"x": True}]),
        ("SET autocommit = 1; SELECT @@autocommit AS x", [{"x": True}]),
        ("SET autocommit = 0; SELECT @@autocommit AS x", [{"x": False}]),
        (
            "SET lower_case_table_names = 1; SELECT @@lower_case_table_names AS x",
            [{"x": 1}],
        ),
        # Simple queries
        ("SELECT 1, 2", [{"_col_0": 1, "_col_1": 2}]),
        # USE
        ("USE db2; SELECT DATABASE()", [{"DATABASE()": "db2"}]),
        # INFORMATION_SCHEMA
        # Many of these queries are sourced from Tableau
        (
            """
        SELECT
          *
        FROM information_schema.schemata
        ORDER BY schema_name""",
            [
                {
                    "catalog_name": "def",
                    "schema_name": schema_name,
                    "default_character_set_name": "utf8mb4",
                    "default_collation_name": "utf8mb4_general_ci",
                    "sql_path": None,
                }
                for schema_name in ["db", "information_schema"]
            ],
        ),
        (
            """
        SELECT
          *
        FROM information_schema.tables
        ORDER BY table_schema, table_name""",
            [
                {
                    "auto_increment": None,
                    "avg_row_length": None,
                    "check_time": None,
                    "checksum": None,
                    "create_options": None,
                    "create_time": None,
                    "data_free": None,
                    "data_length": None,
                    "engine": "MinervaSQL",
                    "index_length": None,
                    "max_data_length": None,
                    "row_format": None,
                    "table_catalog": "def",
                    "table_collation": "utf8mb4_general_ci ",
                    "table_comment": None,
                    "table_name": table_name,
                    "table_rows": None,
                    "table_schema": table_schema,
                    "table_type": table_type,
                    "update_time": None,
                    "version": "1.0",
                }
                for table_name, table_schema, table_type in [
                    ("x", "db", "BASE TABLE"),
                    ("y", "db", "BASE TABLE"),
                    ("character_sets", "information_schema", "SYSTEM TABLE"),
                    ("columns", "information_schema", "SYSTEM TABLE"),
                    ("key_column_usage", "information_schema", "SYSTEM TABLE"),
                    ("referential_constraints", "information_schema", "SYSTEM TABLE"),
                    ("schemata", "information_schema", "SYSTEM TABLE"),
                    ("statistics", "information_schema", "SYSTEM TABLE"),
                    ("tables", "information_schema", "SYSTEM TABLE"),
                ]
            ],
        ),
        (
            """
        SELECT
          table_catalog,
          table_schema,
          table_name,
          column_name,
          ordinal_position,
          data_type
        FROM information_schema.columns
        WHERE table_schema = 'db' AND table_name LIKE 'x' AND column_name NOT LIKE 'a'
        ORDER BY table_schema, table_name, column_name""",
            [
                {
                    "table_catalog": "def",
                    "table_schema": "db",
                    "table_name": "x",
                    "column_name": "b",
                    "ordinal_position": 1,
                    "data_type": "TEXT",
                }
            ],
        ),
        (
            """
        SELECT
          *
        FROM information_schema.key_column_usage""",
            [],
        ),
        (
            """
        SELECT NULL, NULL, NULL, SCHEMA_NAME
        FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE '%'
        ORDER BY SCHEMA_NAME""",
            [
                {
                    "_col_0": None,
                    "_col_1": None,
                    "_col_2": None,
                    "schema_name": schema_name,
                }
                for schema_name in ["db", "information_schema"]
            ],
        ),
        (
            """
        SELECT NULL, NULL, NULL, SCHEMA_NAME
        FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE '%'
        ORDER BY SCHEMA_NAME""",
            [
                {
                    "_col_0": None,
                    "_col_1": None,
                    "_col_2": None,
                    "schema_name": schema_name,
                }
                for schema_name in ["db", "information_schema"]
            ],
        ),
        (
            """
        SELECT TABLE_NAME,TABLE_COMMENT,IF(TABLE_TYPE='BASE TABLE', 'TABLE', TABLE_TYPE),TABLE_SCHEMA 
        FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'db' AND ( TABLE_TYPE='BASE TABLE' OR TABLE_TYPE='VIEW' )  
        ORDER BY TABLE_SCHEMA, TABLE_NAME""",
            [
                {
                    "_col_2": "TABLE",
                    "table_comment": None,
                    "table_name": table_name,
                    "table_schema": "db",
                }
                for table_name in ["x", "y"]
            ],
        ),
        (
            """
        SELECT `table_name`, `column_name`
        FROM `information_schema`.`columns`
        WHERE `data_type`='enum' AND `table_schema`='db'""",
            [],
        ),
        (
            """
        SELECT COLUMN_NAME,
         REFERENCED_TABLE_NAME,
         REFERENCED_COLUMN_NAME,
         ORDINAL_POSITION,
         CONSTRAINT_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE REFERENCED_TABLE_NAME IS NOT NULL
         AND TABLE_NAME = 'x'
         AND TABLE_SCHEMA = 'db'""",
            [],
        ),
        (
            """
        SELECT
          A.REFERENCED_TABLE_SCHEMA AS PKTABLE_CAT,
          NULL AS PKTABLE_SCHEM,
          A.REFERENCED_TABLE_NAME AS PKTABLE_NAME,
          A.REFERENCED_COLUMN_NAME AS PKCOLUMN_NAME,
          A.TABLE_SCHEMA AS FKTABLE_CAT,
          NULL AS FKTABLE_SCHEM,
          A.TABLE_NAME AS FKTABLE_NAME,
          A.COLUMN_NAME AS FKCOLUMN_NAME,
          A.ORDINAL_POSITION AS KEY_SEQ,
          CASE
            WHEN R.UPDATE_RULE = 'CASCADE' THEN 0 
            WHEN R.UPDATE_RULE = 'SET NULL' THEN 2 
            WHEN R.UPDATE_RULE = 'SET DEFAULT' THEN 4 
            WHEN R.UPDATE_RULE = 'SET RESTRICT' THEN 1 
            WHEN R.UPDATE_RULE = 'SET NO ACTION' THEN 3 
            ELSE 3 
          END AS UPDATE_RULE,
          CASE
            WHEN R.DELETE_RULE = 'CASCADE' THEN 0 
            WHEN R.DELETE_RULE = 'SET NULL' THEN 2
            WHEN R.DELETE_RULE = 'SET DEFAULT' THEN 4
            WHEN R.DELETE_RULE = 'SET RESTRICT' THEN 1 
            WHEN R.DELETE_RULE = 'SET NO ACTION' THEN 3 
          ELSE 3 END AS DELETE_RULE,
          A.CONSTRAINT_NAME AS FK_NAME,
          'PRIMARY' AS PK_NAME,
          7 AS DEFERRABILITY 
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE A 
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE D 
        ON (
          D.TABLE_SCHEMA=A.REFERENCED_TABLE_SCHEMA 
          AND D.TABLE_NAME=A.REFERENCED_TABLE_NAME 
          AND D.COLUMN_NAME=A.REFERENCED_COLUMN_NAME) 
        JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS R 
        ON (
          R.CONSTRAINT_NAME = A.CONSTRAINT_NAME 
          AND R.TABLE_NAME = A.TABLE_NAME 
          AND R.CONSTRAINT_SCHEMA = A.TABLE_SCHEMA) 
        WHERE 
          D.CONSTRAINT_NAME='PRIMARY' 
          AND A.TABLE_SCHEMA = 'magic' 
        ORDER BY FKTABLE_CAT, FKTABLE_NAME, KEY_SEQ, PKTABLE_NAME""",
            [],
        ),
        # SHOW
        (
            "show variables",
            [
                {"Value": "False", "Variable_name": "autocommit"},
                {"Value": "utf8mb4", "Variable_name": "character_set_client"},
                {"Value": "utf8mb4", "Variable_name": "character_set_connection"},
                {"Value": "utf8mb4", "Variable_name": "character_set_database"},
                {"Value": "utf8mb4", "Variable_name": "character_set_results"},
                {"Value": "utf8mb4", "Variable_name": "character_set_server"},
                {
                    "Value": "utf8mb4_general_ci",
                    "Variable_name": "collation_connection",
                },
                {"Value": "utf8mb4_general_ci", "Variable_name": "collation_database"},
                {"Value": "utf8mb4_general_ci", "Variable_name": "collation_server"},
                {"Value": "levon_helm", "Variable_name": "external_user"},
                {"Value": "0", "Variable_name": "lower_case_table_names"},
                {"Value": "ANSI", "Variable_name": "sql_mode"},
                {"Value": "READ-COMMITTED", "Variable_name": "transaction_isolation"},
                {"Value": "8.0.29", "Variable_name": "version"},
                {"Value": "mysql-mimic", "Variable_name": "version_comment"},
            ],
        ),
        (
            "SHOW  SESSION  VARIABLES  LIKE 'version_%'",
            [{"Value": "mysql-mimic", "Variable_name": "version_comment"}],
        ),
        ("show index from x", []),
        (
            "show columns from x like '%'",
            [
                {
                    "Default": None,
                    "Extra": None,
                    "Field": "a",
                    "Key": None,
                    "Null": "YES",
                    "Type": "TEXT",
                },
                {
                    "Default": None,
                    "Extra": None,
                    "Field": "b",
                    "Key": None,
                    "Null": "YES",
                    "Type": "TEXT",
                },
            ],
        ),
        (
            "show tables from information_schema like 'k%'",
            [
                {"Table_name": "key_column_usage"},
            ],
        ),
        (
            "show full tables from db like 'x'",
            [{"Table_name": "x", "Table_type": "BASE TABLE"}],
        ),
        ("show databases like '_n%'", [{"Database": "information_schema"}]),
    ],
)
async def test_commands(
    session: MockSession,
    server: MysqlServer,
    query_fixture: QueryFixture,
    sql: str,
    expected: List[Dict[str, Any]],
) -> None:
    result = await query_fixture(sql)
    assert expected == list(result)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sql,  msg",
    [
        (
            "SET GLOBAL sql_mode = 'TRADITIONAL'",
            "Cannot SET variable sql_mode with scope GLOBAL",
        ),
        ("SET @foo = 'bar'", "User-defined variables not supported yet"),
    ],
)
async def test_unsupported_commands(
    session: MockSession,
    server: MysqlServer,
    query_fixture: QueryFixture,
    sql: str,
    msg: str,
) -> None:
    with pytest.raises(Exception) as ctx:
        await query_fixture(sql)
    assert msg in str(ctx.value)
