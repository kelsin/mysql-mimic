import asyncio
import functools
import io
import unittest
import socket
from datetime import date, datetime, timedelta
from functools import partial

import aiomysql
import mysql.connector
from mysql.connector.connection import MySQLCursorPrepared, MySQLCursorDict

from mysql_mimic import MysqlServer, Session
from mysql_mimic.constants import DEFAULT_SERVER_CAPABILITIES
from mysql_mimic.results import ResultColumn, ResultSet
from mysql_mimic.types import Capabilities, ColumnType
from mysql_mimic.charset import CharacterSet


class PreparedDictCursor(MySQLCursorPrepared):
    def fetchall(self):
        rows = super().fetchall()

        if rows is not None:
            return [dict(zip(self.column_names, row)) for row in rows]

        return None


class MockSession(Session):
    def __init__(self):
        super().__init__()
        self.return_value = None
        self.echo = False
        self.connection = None

    async def init(self, connection):
        self.connection = connection

    async def query(self, sql):
        if self.echo:
            return [(sql,)], ["sql"]
        return self.return_value


async def to_thread(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    func_call = functools.partial(func, *args, **kwargs)
    return await loop.run_in_executor(None, func_call)


def get_free_port():
    sock = socket.socket()
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class TestIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # pylint: disable=attribute-defined-outside-init
        self.session = MockSession()

        self.port = get_free_port()
        self.server = MysqlServer(
            session_factory=lambda: self.session,
            capabilities=(
                DEFAULT_SERVER_CAPABILITIES
                | Capabilities.CLIENT_CONNECT_WITH_DB
                | Capabilities.CLIENT_CONNECT_ATTRS
            ),
            port=self.port,
            conn_kwargs={"force_cursor": True},
        )

        await self.server.start_server()
        asyncio.create_task(self.server.serve_forever())
        self.mysql_conn = await to_thread(
            mysql.connector.connect, use_pure=True, port=self.port
        )
        self.aiomysql_conn = await aiomysql.connect(port=self.port)

    async def asyncTearDown(self):
        self.mysql_conn.close()
        await self.aiomysql_conn.ensure_closed()
        self.server.close()
        await self.server.wait_closed()

    async def mysql_query(self, sql, cursor_class=MySQLCursorDict, params=None):
        cursor = await to_thread(self.mysql_conn.cursor, cursor_class=cursor_class)
        await to_thread(cursor.execute, sql, *(p for p in [params] if p))
        result = await to_thread(cursor.fetchall)
        await to_thread(cursor.close)
        return result

    async def aiomysql_query(self, sql, cursor_class=aiomysql.DictCursor, params=None):
        async with self.aiomysql_conn.cursor(cursor_class) as cur:
            await cur.execute(sql, *(p for p in [params] if p))
            return await cur.fetchall()

    async def test_query(self):
        for query in [
            self.mysql_query,
            partial(self.mysql_query, cursor_class=PreparedDictCursor),
            self.aiomysql_query,
        ]:
            for rv, expected in [
                (None, []),
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
                (
                    ResultSet(
                        rows=[("♥".encode("utf-8"),)],
                        columns=[
                            ResultColumn(
                                name="b",
                                character_set=CharacterSet.utf8mb4,
                                type=ColumnType.VARCHAR,
                            )
                        ],
                    ),
                    [{"b": "♥"}],
                ),
                (
                    (
                        [("♥".encode("utf-8"),)],
                        [
                            ResultColumn(
                                name="b",
                                character_set=CharacterSet.utf8mb4,
                                type=ColumnType.VARCHAR,
                            )
                        ],
                    ),
                    [{"b": "♥"}],
                ),
                (([(None,)], ["b"]), [{"b": None}]),
                (([(None, 1, 1)], ["a", "b", "c"]), [{"a": None, "b": 1, "c": 1}]),
                (([(1, None, 1)], ["a", "b", "c"]), [{"a": 1, "b": None, "c": 1}]),
                (([(1, 1, None)], ["a", "b", "c"]), [{"a": 1, "b": 1, "c": None}]),
                (([[None], [1]], ["b"]), [{"b": None}, {"b": 1}]),
            ]:
                self.session.return_value = rv
                result = await query("SELECT b FROM a")
                self.assertEqual(result, expected)

    async def test_prepared_stmt(self):
        self.session.echo = True

        for sql, params, expected in [
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
        ]:
            result = await self.mysql_query(
                sql, cursor_class=PreparedDictCursor, params=params
            )
            self.assertEqual(result[0]["sql"], expected)

    async def test_init(self):
        async with aiomysql.connect(
            port=self.port, user="levon_helm", db="db", program_name="test"
        ):
            connection = self.session.connection
            self.assertEqual(connection.username, "levon_helm")
            self.assertEqual(connection.client_connect_attrs["program_name"], "test")
            self.assertEqual(connection.database, "db")

    async def test_connection_id(self):
        async with aiomysql.connect(port=self.port) as conn1:
            async with aiomysql.connect(port=self.port) as conn2:
                self.assertEqual(
                    conn1.server_thread_id[0] + 1, conn2.server_thread_id[0]
                )

    async def test_replace_variables(self):
        self.session.echo = True

        result = await self.aiomysql_query("SELECT CONNECTION_ID()")
        parts = result[0]["sql"].split(" ")
        self.assertEqual(parts[0], "SELECT")
        self.assertTrue(int(parts[1]))

        result = await self.aiomysql_query("SELECT @@version_comment")
        parts = result[0]["sql"].split(" ")
        self.assertEqual(parts[0], "SELECT")
        self.assertEqual(parts[1], "'mysql-mimic'")
