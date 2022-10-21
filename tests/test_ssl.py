import os
from contextlib import closing
from ssl import SSLContext, PROTOCOL_TLS_SERVER

import pytest

from mysql_mimic import MysqlServer
from tests.conftest import query, MockSession, ConnectFixture

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
    server: MysqlServer,
    session: MockSession,
    connect: ConnectFixture,
) -> None:
    session.use_sqlite = True

    with closing(await connect()) as conn:
        assert await query(conn=conn, sql="SELECT 1 AS a") == [{"a": 1}]

    with closing(await connect(ssl_disabled=True)) as conn:
        assert await query(conn=conn, sql="SELECT 1 AS a") == [{"a": 1}]
