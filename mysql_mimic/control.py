from __future__ import annotations

import random
from typing import Dict, Optional, TYPE_CHECKING

from mysql_mimic.constants import KillKind
from mysql_mimic.utils import seq

if TYPE_CHECKING:
    from mysql_mimic.connection import Connection


class TooManyConnections(Exception):
    pass


class Control:
    """
    Base class for controlling server state.

    This is intended to encapsulate operations that might span deployment of multiple
    MySQL mimic server instances.
    """

    async def add(self, connection: Connection) -> int:
        """
        Add a new connection

        Returns:
            connection id
        """
        raise NotImplementedError()

    async def remove(self, connection_id: int) -> None:
        """Remove an existing connection"""
        raise NotImplementedError()

    async def kill(
        self, connection_id: int, kind: KillKind = KillKind.CONNECTION
    ) -> None:
        """Request termination of an existing connection"""
        raise NotImplementedError()


class LocalControl(Control):
    """
    Simple Control implementation that handles everything in-memory.

    Args:
        server_id: set a unique server ID. This is used to generate globally unique
            connection IDs. This should be an integer between 0 and 65535.
            If left as None, a random server ID will be generated.
    """

    _CONNECTION_ID_BITS = 16
    _MAX_CONNECTION_SEQ = 2**_CONNECTION_ID_BITS
    _MAX_SERVER_ID = 2**16

    def __init__(self, server_id: Optional[int] = None):
        self._connection_seq = seq(self._MAX_CONNECTION_SEQ)
        self._connections: Dict[int, Connection] = {}
        self.server_id = server_id or random.randint(0, self._MAX_SERVER_ID - 1)

    async def add(self, connection: Connection) -> int:
        connection_id = self._new_connection_id()
        self._connections[connection_id] = connection
        return connection_id

    async def remove(self, connection_id: int) -> None:
        self._connections.pop(connection_id, None)

    async def kill(
        self, connection_id: int, kind: KillKind = KillKind.CONNECTION
    ) -> None:
        conn = self._connections.get(connection_id)
        if conn:
            conn.kill(kind)

    def _new_connection_id(self) -> int:
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
            raise TooManyConnections()

        server_id_prefix = (
            self.server_id % self._MAX_SERVER_ID
        ) << self._CONNECTION_ID_BITS

        connection_id = server_id_prefix + next(self._connection_seq)

        # Avoid connection ID collisions in the unlikely chance that a connection is
        # alive longer than it takes for the sequence to reset.
        while connection_id in self._connections:
            connection_id = server_id_prefix + next(self._connection_seq)

        return connection_id
