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
            # Sqlalchemy fires off a bunch of metadata queries when connecting.
            # We'll just route things like "SELECT VERSION()" to sqlite, which should be
            # fine because the connection replaces "VERSION()" with the version name.
            session.use_sqlite = True
            async with sqlalchemy_engine.connect() as conn:
                session.use_sqlite = False
                cursor = await conn.execute(text(sql))
                return cursor.mappings().all()

        return q4

    raise Exception("Unexpected fixture param")


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
        ("SELECT ?", ("1",), "SELECT '1'"),
        ("SELECT '?'", (), "SELECT '?'"),
        ("SELECT ?", (1,), "SELECT 1"),
        ("SELECT ?", (0,), "SELECT 0"),
        ("SELECT ?", (-1,), "SELECT -1"),
        ("SELECT ?", (255,), "SELECT 255"),
        ("SELECT ?", (-128,), "SELECT -128"),
        ("SELECT ?", (65535,), "SELECT 65535"),
        ("SELECT ?", (-32767,), "SELECT -32767"),
        ("SELECT ?", (4294967295,), "SELECT 4294967295"),
        ("SELECT ?", (-2147483648,), "SELECT -2147483648"),
        ("SELECT ?", (18446744073709551615,), "SELECT 18446744073709551615"),
        ("SELECT ?", (-9223372036854775808,), "SELECT -9223372036854775808"),
        ("SELECT ?", (1.1,), "SELECT 1.1"),
        ("SELECT ?", (1.7e308,), "SELECT 1.7e+308"),
        ("SELECT ?", (None,), "SELECT NULL"),
        ("SELECT ?", (b"hello",), "SELECT 'hello'"),
        ("SELECT ?", (io.BytesIO(b"hello"),), "SELECT 'hello'"),
        ("SELECT ?, ?", ("1", "1"), "SELECT '1', '1'"),
        (
            "SELECT ?, ?, ?, ?",
            ("1", None, io.BytesIO(b"hello"), 1),
            "SELECT '1', NULL, 'hello', 1",
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
async def test_replace_variables(
    session: MockSession, server: MysqlServer, connect: ConnectFixture
) -> None:
    session.echo = True

    with closing(await connect(user="levon_helm")) as conn:
        result = await query(conn, "SELECT CONNECTION_ID()")
        parts = result[0]["sql"].split(" ")
        assert parts[0] == "SELECT"
        int(parts[1])  # no error raised

        result = await query(conn, "SELECT @@version_comment")
        parts = result[0]["sql"].split(" ")
        assert parts[0] == "SELECT"
        assert parts[1] == "'mysql-mimic'"

        result = await query(conn, "SELECT CURRENT_USER")
        parts = result[0]["sql"].split(" ")
        assert parts[0] == "SELECT"
        assert parts[1] == "'levon_helm'"


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
        sql = "SELECT 1"
        query_attrs = {
            "idx": i,
            "str": "foo",
            "int": 1,
            "float": 1.1,
        }
        result = await q(sql=sql, query_attributes=query_attrs)
        assert session.last_query_attrs == query_attrs
        assert result[0]["sql"] == sql
