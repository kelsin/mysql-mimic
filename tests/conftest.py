import asyncio
import functools
import socket
import sqlite3
from ssl import SSLContext
from typing import (
    Optional,
    List,
    Dict,
    Any,
    Callable,
    TypeVar,
    Awaitable,
    Sequence,
    AsyncGenerator,
    Type,
    Tuple,
)

import aiomysql
import mysql.connector
import sqlalchemy.engine
from mysql.connector.connection import (
    MySQLCursorPrepared,
    MySQLCursorDict,
    MySQLConnection,
)
from mysql.connector.cursor import MySQLCursor
from sqlalchemy.ext.asyncio import create_async_engine
import pytest
import pytest_asyncio

from mysql_mimic import MysqlServer, Session
from mysql_mimic.auth import (
    User,
    AuthPlugin,
    IdentityProvider,
)
from mysql_mimic.connection import Connection
from mysql_mimic.results import AllowedResult, Column


class PreparedDictCursor(MySQLCursorPrepared):
    def fetchall(self) -> Optional[List[Dict[str, Any]]]:
        rows = super().fetchall()

        if rows is not None:
            return [dict(zip(self.column_names, row)) for row in rows]

        return None


class MockSession(Session):
    def __init__(self) -> None:
        super().__init__()
        self.return_value: Any = None
        self.echo = False
        self.sqlite = sqlite3.connect(":memory:")
        self.use_sqlite = False
        self.connection: Optional[Connection] = None
        self.last_query_attrs: Optional[Dict[str, str]] = None
        self.users: Optional[Dict[str, User]] = None
        self.columns: Dict[Tuple[str, str], List[dict]] = {}

    async def init(self, connection: Connection) -> None:
        self.connection = connection

    async def query(self, sql: str, attrs: Dict[str, str]) -> AllowedResult:
        self.last_query_attrs = attrs
        if self.use_sqlite:
            cursor = self.sqlite.execute(sql)
            return cursor.fetchall(), [d[0] for d in cursor.description]
        if self.echo:
            return [(sql,)], ["sql"]
        return self.return_value

    async def show_columns(self, database: str, table: str) -> Sequence[dict]:
        return self.columns.get((database, table), [])

    async def show_tables(self, database: str) -> Sequence[str]:
        return [table for db, table in self.columns if db == database]

    async def show_databases(self) -> Sequence[str]:
        return [db for db, _ in self.columns]


class MockIdentityProvider(IdentityProvider):
    def __init__(self, auth_plugins: List[AuthPlugin], users: Dict[str, User]):
        self.auth_plugins = auth_plugins
        self.users = users

    def get_plugins(self) -> Sequence[AuthPlugin]:
        return self.auth_plugins

    async def get_user(self, username: str) -> Optional[User]:
        return self.users.get(username)


T = TypeVar("T")


async def to_thread(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    loop = asyncio.get_running_loop()
    func_call = functools.partial(func, *args, **kwargs)
    return await loop.run_in_executor(None, func_call)


def get_free_port() -> int:
    sock = socket.socket()
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


@pytest.fixture
def session() -> MockSession:
    return MockSession()


@pytest.fixture
def port() -> int:
    return get_free_port()


@pytest.fixture
def auth_plugins() -> Optional[List[AuthPlugin]]:
    return None


@pytest.fixture
def users() -> Dict[str, User]:
    return {}


@pytest.fixture
def identity_provider(
    auth_plugins: Optional[List[AuthPlugin]], users: Dict[str, User]
) -> Optional[MockIdentityProvider]:
    if auth_plugins:
        return MockIdentityProvider(auth_plugins, users)
    return None


@pytest.fixture
def ssl() -> Optional[SSLContext]:
    return None


@pytest_asyncio.fixture
async def server(
    session: MockSession,
    port: int,
    identity_provider: Optional[MockIdentityProvider],
    ssl: Optional[SSLContext],
) -> AsyncGenerator[MysqlServer, None]:
    srv = MysqlServer(
        session_factory=lambda: session,
        port=port,
        identity_provider=identity_provider,
        ssl=ssl,
    )
    await srv.start_server()
    asyncio.create_task(srv.serve_forever())
    try:
        yield srv
    finally:
        srv.close()
        await srv.wait_closed()


ConnectFixture = Callable[..., Awaitable[MySQLConnection]]


@pytest.fixture
def connect(port: int) -> ConnectFixture:
    async def conn(**kwargs: Any) -> MySQLConnection:
        return await to_thread(
            mysql.connector.connect, use_pure=True, port=port, **kwargs
        )

    return conn


@pytest_asyncio.fixture
async def mysql_connector_conn(connect: ConnectFixture) -> MySQLConnection:
    conn = await connect()
    try:
        yield conn
    finally:
        conn.close()


@pytest_asyncio.fixture
async def aiomysql_conn(port: int) -> aiomysql.Connection:
    async with aiomysql.connect(port=port) as conn:
        yield conn


@pytest_asyncio.fixture
async def sqlalchemy_engine(port: int) -> sqlalchemy.engine.Engine:
    engine = create_async_engine(url=f"mysql+aiomysql://127.0.0.1:{port}")
    try:
        yield engine
    finally:
        await engine.dispose()


async def query(
    conn: MySQLConnection,
    sql: str,
    cursor_class: Type[MySQLCursor] = MySQLCursorDict,
    params: Sequence[Any] = None,
    query_attributes: Dict[str, str] = None,
) -> Sequence[Any]:
    cursor = await to_thread(conn.cursor, cursor_class=cursor_class)
    if query_attributes:
        for key, value in query_attributes.items():
            cursor.add_attribute(key, value)
    await to_thread(cursor.execute, sql, *(p for p in [params] if p))
    result = await to_thread(cursor.fetchall)
    await to_thread(cursor.close)
    return result