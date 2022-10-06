import asyncio
import functools
import io
import unittest
import socket
import sqlite3
from datetime import date, datetime, timedelta
from functools import partial

import aiomysql
import mysql.connector
from mysql.connector.connection import MySQLCursorPrepared, MySQLCursorDict
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

from mysql_mimic import MysqlServer, Session
from mysql_mimic.results import ResultColumn, ResultSet
from mysql_mimic.types import ColumnType
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
        self.sqlite = sqlite3.connect(":memory:")
        self.use_sqlite = False
        self.connection = None
        self.last_query_attrs = None

    async def init(self, connection):
        self.connection = connection

    async def query(self, sql, attrs):
        self.last_query_attrs = attrs
        if self.use_sqlite:
            cursor = self.sqlite.execute(sql)
            return cursor.fetchall(), [d[0] for d in cursor.description]
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
            port=self.port,
        )

        await self.server.start_server()
        asyncio.create_task(self.server.serve_forever())
        self.mysql_conn = await to_thread(
            mysql.connector.connect, use_pure=True, port=self.port
        )
        self.aiomysql_conn = await aiomysql.connect(port=self.port)
        self.sqlalchemy_engine = create_async_engine(
            url=f"mysql+aiomysql://127.0.0.1:{self.port}"
        )

    async def asyncTearDown(self):
        self.mysql_conn.close()
        await self.aiomysql_conn.ensure_closed()
        self.server.close()
        await self.server.wait_closed()

    async def mysql_query(
        self, sql, cursor_class=MySQLCursorDict, params=None, query_attributes=None
    ):
        cursor = await to_thread(self.mysql_conn.cursor, cursor_class=cursor_class)
        if query_attributes:
            for key, value in query_attributes.items():
                cursor.add_attribute(key, value)
        await to_thread(cursor.execute, sql, *(p for p in [params] if p))
        result = await to_thread(cursor.fetchall)
        await to_thread(cursor.close)
        return result

    async def aiomysql_query(self, sql):
        async with self.aiomysql_conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql)
            return await cur.fetchall()

    async def sqlalchemy_query(self, sql):
        # Sqlalchemy fires off a bunch of metadata queries when connecting.
        # We'll just route things like "SELECT VERSION()" to sqlite, which should be
        # fine because the connection replaces "VERSION()" with the version name.
        self.session.use_sqlite = True
        async with self.sqlalchemy_engine.connect() as conn:
            self.session.use_sqlite = False
            cursor = await conn.execute(text(sql))
            return cursor.mappings().all()

    async def test_query(self):
        explicit_types = [
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
        custom_encoders = [
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
            )
        ]

        for query in [
            self.mysql_query,
            partial(self.mysql_query, cursor_class=PreparedDictCursor),
            self.aiomysql_query,
            self.sqlalchemy_query,
        ]:
            for rv, expected in [
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
                *explicit_types,
                *custom_encoders,
            ]:
                self.session.return_value = rv
                result = await query("SELECT b FROM a")
                self.assertEqual(expected, result)

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

    async def test_query_attributes(self):
        self.session.echo = True

        for i, query in enumerate(
            [
                self.mysql_query,
                partial(self.mysql_query, cursor_class=PreparedDictCursor),
            ]
        ):
            self.session.last_query_attrs = None
            sql = "SELECT 1"
            query_attrs = {
                "idx": i,
                "str": "foo",
                "int": 1,
                "float": 1.1,
            }
            result = await query(sql, query_attributes=query_attrs)
            self.assertEqual(self.session.last_query_attrs, query_attrs)
            self.assertEqual(result[0]["sql"], sql)
