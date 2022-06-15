import asyncio
import random
import itertools

from mysql_mimic.connection import Connection
from mysql_mimic.session import Session
from mysql_mimic.constants import DEFAULT_SERVER_CAPABILITIES
from mysql_mimic.stream import MysqlStream


class MaxConnectionsExceeded(Exception):
    pass


class MysqlServer:
    """
    MySQL mimic server.

    Args:
        session_factory (()->Session): Callable that takes no arguments and returns a session
        capabilities (int): server capability flags
        server_id (int): set a unique server ID. This is used to generate globally unique
            connection IDs. This should be an integer between 0 and 65535.
            If left as None, a random server ID will be generated.
        **kwargs: extra keyword args passed to the asyncio start server command
    """

    _CONNECTION_ID_BITS = 16
    _MAX_CONNECTION_SEQ = 2**_CONNECTION_ID_BITS
    _MAX_SERVER_ID = 2**16

    def __init__(
        self,
        session_factory=Session,
        capabilities=DEFAULT_SERVER_CAPABILITIES,
        server_id=None,
        **kwargs,
    ):
        self.session_factory = session_factory
        self.capabilities = capabilities
        self.server_id = server_id or self._get_server_id()
        self._connection_seq = itertools.count()
        self._connections = {}
        self._serve_kwargs = kwargs
        self._server = None

    async def _client_connected_cb(self, reader, writer):
        connection_id = self._get_connection_id()
        connection = Connection(
            stream=MysqlStream(reader, writer),
            session_factory=self.session_factory,
            server_capabilities=self.capabilities,
            connection_id=connection_id,
        )
        self._connections[connection_id] = connection
        try:
            return await connection.start()
        finally:
            self._connections.pop(connection_id, None)

    def _get_server_id(self):
        return random.randint(0, self._MAX_SERVER_ID - 1)

    def _get_connection_id(self):
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

        connection_id = server_id_prefix + self._next_connection_sequence()

        # Avoid connection ID collisions in the unlikely chance that a connection is
        # alive longer than it takes for the sequence to reset.
        while connection_id in self._connections:
            connection_id = server_id_prefix + self._next_connection_sequence()

        return connection_id

    def _next_connection_sequence(self):
        conn_seq = next(self._connection_seq)
        if conn_seq >= self._MAX_CONNECTION_SEQ:
            self._connection_seq = itertools.count()
        return conn_seq

    async def start_server(self, **kwargs):
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

    async def start_unix_server(self, **kwargs):
        """
        Start an asyncio unix socket server.

        Args:
            **kwargs: keyword args passed to `asyncio.start_unix_server`
        """
        kw = {}
        kw.update(self._serve_kwargs)
        kw.update(kwargs)
        self._server = await asyncio.start_unix_server(self._client_connected_cb, **kw)

    async def serve_forever(self, **kwargs):
        """
        Start accepting connections until the coroutine is cancelled.

        If a server isn't running, this starts a server with `start_server`.

        Args:
            **kwargs: keyword args passed to `start_server`
        """
        if not self._server:
            await self.start_server(**kwargs)
        await self._server.serve_forever()

    def close(self):
        """
        Stop serving.

        The server is closed asynchronously -
        use the `wait_closed` coroutine to wait until the server is closed.
        """
        if self._server:
            self._server.close()

    async def wait_closed(self):
        """Wait until the `close` method completes."""
        if self._server:
            await self._server.wait_closed()
