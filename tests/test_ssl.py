import os
from ssl import SSLContext, PROTOCOL_TLS_SERVER, create_default_context

import aiomysql
import pytest

from tests.conftest import MockSession, ConnectFixture

CURRENT_DIR = os.path.dirname(__file__)
SSL_CERT = os.path.join(CURRENT_DIR, "fixtures/certificate.pem")
SSL_KEY = os.path.join(CURRENT_DIR, "fixtures/key.pem")


@pytest.fixture(autouse=True)
def ssl() -> SSLContext:
    sslcontext = SSLContext(PROTOCOL_TLS_SERVER)
    sslcontext.load_cert_chain(certfile=SSL_CERT, keyfile=SSL_KEY)
    return sslcontext


@pytest.mark.asyncio
async def test_ssl(
    session: MockSession,
    connect: ConnectFixture,
    port: int,
) -> None:
    session.use_sqlite = True

    async with aiomysql.connect(port=port, ssl=create_default_context()) as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT 1 AS a")
            assert await cur.fetchall() == [{"a": 1}]
