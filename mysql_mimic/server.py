from __future__ import annotations

import asyncio
import random
from ssl import SSLContext
from socket import socket
from typing import Callable, Any, Dict, Optional, Sequence

from mysql_mimic.auth import IdentityProvider, SimpleIdentityProvider
from mysql_mimic.connection import Connection
from mysql_mimic.session import Session, BaseSession
from mysql_mimic.constants import DEFAULT_SERVER_CAPABILITIES
from mysql_mimic.stream import MysqlStream
from mysql_mimic.types import Capabilities
from mysql_mimic.utils import seq


class MaxConnectionsExceeded(Exception):
    pass


class MysqlServer:
    """
    MySQL mimic server.

    Args:
        session_factory: Callable that takes no arguments and returns a session
        capabilities: server capability flags
        server_id: set a unique server ID. This is used to generate globally unique
            connection IDs. This should be an integer between 0 and 65535.
            If left as None, a random server ID will be generated.
        identity_provider: Authentication plugins to register. Defaults to `SimpleIdentityProvider`,
            which just blindly accepts whatever `username` is given by the client.
        ssl: SSLContext instance if this server should enable TLS over connections

        **kwargs: extra keyword args passed to the asyncio start server command
    """

    _CONNECTION_ID_BITS = 16
    _MAX_CONNECTION_SEQ = 2**_CONNECTION_ID_BITS
    _MAX_SERVER_ID = 2**16

    def __init__(
        self,
        session_factory: Callable[[], BaseSession] = Session,
        capabilities: Capabilities = DEFAULT_SERVER_CAPABILITIES,
        server_id: int | None = None,
        identity_provider: IdentityProvider | None = None,
        ssl: SSLContext | None = None,
        **serve_kwargs: Any,
    ):
        self.session_factory = session_factory
        self.capabilities = capabilities
        self.server_id = server_id or self._get_server_id()
        self.identity_provider = identity_provider or SimpleIdentityProvider()
        self.ssl = ssl

        self._connection_seq = seq(self._MAX_CONNECTION_SEQ)
        self._connections: Dict[int, Connection] = {}
        self._serve_kwargs = serve_kwargs
        self._server: Optional[asyncio.base_events.Server] = None

    async def _client_connected_cb(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        connection_id = self._get_connection_id()
        connection = Connection(
            stream=MysqlStream(reader, writer),
            session=self.session_factory(),
            server_capabilities=self.capabilities,
            connection_id=connection_id,
            identity_provider=self.identity_provider,
            ssl=self.ssl,
        )
        self._connections[connection_id] = connection
        try:
            return await connection.start()
        finally:
            self._connections.pop(connection_id, None)

    def _get_server_id(self) -> int:
        return random.randint(0, self._MAX_SERVER_ID - 1)

    def _get_connection_id(self) -> int:
        """
        Generate a connection ID.

        MySQL connection IDs are 4 bytes.

        This is tricky for us, as there may be multiple MySQL-Mimic server instances.

        We use a concatenation of the server ID (first two bytes) and connection
        sequence (second two bytes):

        |<---server ID--->|<-conn sequence->|
         00000000 00000000 00000000 00000000

        If incremental connection IDs aren't manually provided, this isn't guaranteed
        to be unique. But collisions should be highly unlikely.
        """
        if len(self._connections) >= self._MAX_CONNECTION_SEQ:
            raise MaxConnectionsExceeded()
        server_id_prefix = (
            self.server_id % self._MAX_SERVER_ID
        ) << self._CONNECTION_ID_BITS

        connection_id = server_id_prefix + next(self._connection_seq)

        # Avoid connection ID collisions in the unlikely chance that a connection is
        # alive longer than it takes for the sequence to reset.
        while connection_id in self._connections:
            connection_id = server_id_prefix + next(self._connection_seq)

        return connection_id

    async def start_server(self, **kwargs: Any) -> None:
        """
        Start an asyncio socket server.

        Args:
            **kwargs: keyword args passed to `asyncio.start_server`
        """
        kw = {}
        kw.update(self._serve_kwargs)
        kw.update(kwargs)
        if "port" not in kw:
            kw["port"] = 3306
        self._server = await asyncio.start_server(self._client_connected_cb, **kw)

    async def start_unix_server(self, **kwargs: Any) -> None:
        """
        Start an asyncio unix socket server.

        Args:
            **kwargs: keyword args passed to `asyncio.start_unix_server`
        """
        kw = {}
        kw.update(self._serve_kwargs)
        kw.update(kwargs)
        self._server = await asyncio.start_unix_server(self._client_connected_cb, **kw)

    async def serve_forever(self, **kwargs: Any) -> None:
        """
        Start accepting connections until the coroutine is cancelled.

        If a server isn't running, this starts a server with `start_server`.

        Args:
            **kwargs: keyword args passed to `start_server`
        """
        if not self._server:
            await self.start_server(**kwargs)
        assert self._server is not None
        await self._server.serve_forever()

    def close(self) -> None:
        """
        Stop serving.

        The server is closed asynchronously -
        use the `wait_closed` coroutine to wait until the server is closed.
        """
        if self._server:
            self._server.close()

    async def wait_closed(self) -> None:
        """Wait until the `close` method completes."""
        if self._server:
            await self._server.wait_closed()

    def sockets(self) -> Sequence[socket]:
        """Get sockets the server is listening on."""
        if self._server:
            return self._server.sockets
        return ()
