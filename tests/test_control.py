import asyncio
from contextlib import closing
from typing import Callable

import pytest
from mysql.connector import DatabaseError

from tests.conftest import MockSession, query, ConnectFixture


@pytest.fixture
def session_factory(session: MockSession) -> Callable[[], MockSession]:
    called_once = False

    def factory() -> MockSession:
        nonlocal called_once
        if not called_once:
            called_once = True
            return session
        return MockSession()

    return factory


@pytest.mark.asyncio
async def test_kill_connection(
    session: MockSession,
    connect: ConnectFixture,
) -> None:
    session.pause = asyncio.Event()

    with closing(await connect()) as conn1:
        with closing(await connect()) as conn2:
            q1 = asyncio.create_task(query(conn1, "SELECT a FROM x"))

            # Wait until we know the session is paused
            await session.waiting.wait()

            await query(conn2, f"KILL {session.connection.connection_id}")

            with pytest.raises(DatabaseError) as ctx:
                await q1

            assert "Session was killed" in str(ctx.value.msg)

            session.pause.clear()

            with pytest.raises(DatabaseError) as ctx:
                await query(conn1, "SELECT 1")

            assert "Connection not available" in str(ctx.value.msg)


@pytest.mark.asyncio
async def test_kill_query(
    session: MockSession,
    connect: ConnectFixture,
) -> None:
    session.pause = asyncio.Event()

    with closing(await connect()) as conn1:
        with closing(await connect()) as conn2:
            q1 = asyncio.create_task(query(conn1, "SELECT a FROM x"))

            # Wait until we know the session is paused
            await session.waiting.wait()

            await query(conn2, f"KILL QUERY {session.connection.connection_id}")

            with pytest.raises(DatabaseError) as ctx:
                await q1

            assert "Query was killed" in str(ctx.value.msg)

            session.pause.clear()

            assert (await query(conn1, "SELECT 1")) == [{"1": 1}]
