import asyncio
import unittest
import socket
from datetime import date, datetime, timedelta
from unittest.mock import Mock

import aiomysql

from mysql_mimic import MysqlServer, Session
from mysql_mimic.constants import DEFAULT_SERVER_CAPABILITIES
from mysql_mimic.result import ResultColumn, ResultSet
from mysql_mimic.types import Capabilities, CharacterSet, ColumnType


def get_free_port():
    sock = socket.socket()
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class TestIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # pylint: disable=attribute-defined-outside-init
        self.session = Mock(spec=Session)

        port = get_free_port()
        self.server = MysqlServer(
            session_factory=lambda: self.session,
            capabilities=(
                DEFAULT_SERVER_CAPABILITIES
                | Capabilities.CLIENT_CONNECT_WITH_DB
                | Capabilities.CLIENT_CONNECT_ATTRS
            ),
            port=port,
        )
        await self.server.start_server()
        asyncio.create_task(self.server.serve_forever())
        self.pool = await aiomysql.create_pool(
            port=port, user="levon_helm", db="db", program_name="test"
        )

    async def asyncTearDown(self):
        self.pool.close()
        await self.pool.wait_closed()
        self.server.close()
        await self.server.wait_closed()

    async def query(self, sql):
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql)
                return await cur.fetchall()

    async def test_query(self):
        for rv, expected in [
            (([], []), ()),  # Not sure why the aiomysql result is a tuple
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
                            character_set=CharacterSet.UTF8,
                            type=ColumnType.VARCHAR,
                            encoder=lambda x: x,
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
                            character_set=CharacterSet.UTF8,
                            type=ColumnType.VARCHAR,
                            encoder=lambda x: x,
                        )
                    ],
                ),
                [{"b": "♥"}],
            ),
            (([(None,)], ["b"]), [{"b": None}]),
            (([[None], [1]], ["b"]), [{"b": None}, {"b": 1}]),
        ]:
            self.session.query.return_value = rv
            result = await self.query("SELECT b FROM a")
            self.assertEqual(result, expected)

    async def test_init(self):
        connection = self.session.init.mock_calls[0].args[0]
        self.assertEqual(connection.username, "levon_helm")
        self.assertEqual(connection.client_connect_attrs["program_name"], "test")
        self.assertEqual(connection.database, "db")

    async def test_connection_id(self):
        async with self.pool.acquire() as conn1:
            async with self.pool.acquire() as conn2:
                self.assertEqual(
                    conn1.server_thread_id[0] + 1, conn2.server_thread_id[0]
                )
