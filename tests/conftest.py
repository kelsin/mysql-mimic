import asyncio
import functools
import socket
import sqlite3

import aiomysql
import mysql.connector
from mysql.connector.connection import MySQLCursorPrepared, MySQLCursorDict
from mysql.connector.plugins.mysql_clear_password import MySQLClearPasswordAuthPlugin
from sqlalchemy.ext.asyncio import create_async_engine
import pytest
import pytest_asyncio

from mysql_mimic import MysqlServer, Session
from mysql_mimic.auth import (
    User,
    AbstractMysqlClearPasswordAuthPlugin,
)


# mysql.connector throws an error if you try to use mysql_clear_password without SSL.
# That's silly, since SSL termination doesn't have to be handled by MySQL.
# But it's extra silly in tests.
MySQLClearPasswordAuthPlugin.requires_ssl = False


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
        self.users = None

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

    async def get_user(self, username):
        if not self.users:
            return User(name=username)
        return self.users.get(username)


class TestPlugin(AbstractMysqlClearPasswordAuthPlugin):
    name = "test_plugin"

    async def check(self, username, password):
        return username == password


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


@pytest.fixture
def session():
    return MockSession()


@pytest.fixture
def port():
    return get_free_port()


@pytest.fixture
def auth_plugins():
    return None


@pytest_asyncio.fixture
async def server(session, port, auth_plugins):
    srv = MysqlServer(
        session_factory=lambda: session,
        port=port,
        auth_plugins=auth_plugins,
    )
    await srv.start_server()
    asyncio.create_task(srv.serve_forever())
    try:
        yield srv
    finally:
        srv.close()
        await srv.wait_closed()


@pytest.fixture
def connect(port):
    async def conn(**kwargs):
        return await to_thread(
            mysql.connector.connect, use_pure=True, port=port, **kwargs
        )

    return conn


@pytest_asyncio.fixture
async def mysql_connector_conn(connect):
    conn = await connect()
    try:
        yield conn
    finally:
        conn.close()


@pytest_asyncio.fixture
async def aiomysql_conn(port):
    conn = await aiomysql.connect(port=port)
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def sqlalchemy_engine(port):
    return create_async_engine(url=f"mysql+aiomysql://127.0.0.1:{port}")


async def query(
    conn, sql, cursor_class=MySQLCursorDict, params=None, query_attributes=None
):
    cursor = await to_thread(conn.cursor, cursor_class=cursor_class)
    if query_attributes:
        for key, value in query_attributes.items():
            cursor.add_attribute(key, value)
    await to_thread(cursor.execute, sql, *(p for p in [params] if p))
    result = await to_thread(cursor.fetchall)
    await to_thread(cursor.close)
    return result
