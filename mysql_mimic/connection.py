import io
import logging

from mysql_mimic import types
from mysql_mimic.session import Session
from mysql_mimic.constants import DEFAULT_SERVER_CAPABILITIES
from mysql_mimic.result import ensure_result_set
from mysql_mimic.types import CharacterSet
from mysql_mimic.version import __version__ as VERSION


logger = logging.getLogger(__name__)


class Connection:
    def __init__(
        self,
        stream,
        connection_id,
        session_factory=Session,
        server_capabilities=DEFAULT_SERVER_CAPABILITIES,
    ):
        """
        Client connection.

        Args:
            stream (mysql_mimic.stream.MysqlStream): stream to use for writing/reading
            connection_id (int): 32 bit connection ID
            session_factory (()->Session): Callable that takes no arguments and returns a session
            server_capabilities (int): server capability flags
        """
        self.stream = stream
        self.session_factory = session_factory
        self.connection_id = connection_id

        self.server_capabilities = server_capabilities
        self.client_capabilities = types.Capabilities(0)
        self.capabilities = types.Capabilities(0)
        self.status_flags = types.ServerStatus(0)

        self.max_packet_size = 0
        self.server_character_set = 33
        self.client_character_set = 0
        self.username = None
        self.auth_response = None
        self.database = None
        self.client_plugin_name = None
        self.client_connect_attrs = {}
        self.zstd_compression_level = 0

    def ok(self, eof=False, affected_rows=0, last_insert_id=0, warnings=0):
        data = types.int_1(0) if not eof else types.int_1(0xFE)
        data += types.int_len(affected_rows) + types.int_len(last_insert_id)

        if types.Capabilities.CLIENT_PROTOCOL_41 in self.capabilities:
            data += types.int_2(self.status_flags)
            data += types.int_2(warnings)
        elif types.Capabilities.CLIENT_TRANSACTIONS in self.capabilities:
            data += types.int_2(self.status_flags)

        return data

    def eof(self, warnings=0):
        data = types.int_1(0xFE)

        if types.Capabilities.CLIENT_PROTOCOL_41 in self.capabilities:
            data += types.int_2(warnings)
            data += types.int_2(self.status_flags)

        return data

    def error(
        self,
        msg,
        code=1105,
        sql_state_marker=b"#",
        sql_state=b"HY000",
    ):
        data = types.int_1(0xFF) + types.int_2(code)

        if types.Capabilities.CLIENT_PROTOCOL_41 in self.capabilities:
            data += types.str_fixed(1, sql_state_marker)
            data += types.str_fixed(5, sql_state)

        data += types.str_rest(str(msg).encode("utf-8"))

        return data

    # pylint: disable=too-many-arguments
    def column_definition_41(
        self,
        schema=None,
        table=None,
        org_table=None,
        name=None,
        org_name=None,
        character_set=CharacterSet.UTF8,
        column_length=256,
        column_type=types.ColumnType.VARCHAR,
        flags=types.ColumnDefinition(0),
        decimals=0,
    ):
        schema = schema or ""
        table = table or ""
        org_table = org_table or table
        name = name or ""
        org_name = org_name or name

        return (
            types.str_len(b"def")
            + types.str_len(schema.encode("utf-8"))
            + types.str_len(table.encode("utf-8"))
            + types.str_len(org_table.encode("utf-8"))
            + types.str_len(name.encode("utf-8"))
            + types.str_len(org_name.encode("utf-8"))
            + types.int_len(0x0C)  # Length of the following fields
            + types.int_2(character_set)
            + types.int_4(column_length)
            + types.int_1(column_type)
            + types.int_2(flags)
            + types.int_1(decimals)
            + types.int_2(0)  # filler
        )

    def handshake_response_41(self, data):
        r = io.BytesIO(data)
        self.client_capabilities = types.Capabilities(types.read_int_4(r))
        self.capabilities = self.server_capabilities & self.client_capabilities
        self.max_packet_size = types.read_int_4(r)
        self.client_character_set = types.read_int_1(r)
        types.read_str_fixed(r, 23)
        self.username = types.read_str_null(r).decode()

        if (
            types.Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
            in self.capabilities
        ):
            self.auth_response = types.read_str_len(r).decode()
        else:
            l_auth_response = types.read_int_1(r)
            self.auth_response = types.read_str_fixed(r, l_auth_response).decode()

        if types.Capabilities.CLIENT_CONNECT_WITH_DB in self.capabilities:
            self.database = types.read_str_null(r).decode()

        if types.Capabilities.CLIENT_PLUGIN_AUTH in self.capabilities:
            self.client_plugin_name = types.read_str_null(r).decode()

        if types.Capabilities.CLIENT_CONNECT_ATTRS in self.capabilities:
            total_l = types.read_int_len(r)

            while total_l > 0:
                key = types.read_str_len(r)
                value = types.read_str_len(r)
                self.client_connect_attrs[key.decode()] = value.decode()

                item_l = len(types.str_len(key) + types.str_len(value))
                total_l -= item_l

        if types.Capabilities.CLIENT_ZSTD_COMPRESSION_ALGORITHM in self.capabilities:
            self.zstd_compression_level = types.read_int_1(r)

    def handshake_v10(self):
        return (
            types.int_1(10)  # Always 10
            + types.str_null(VERSION.encode("utf-8"))  # Status
            + types.int_4(self.connection_id)  # Connection ID
            + types.str_null(bytes(8))  # plugin data
            + types.int_2(self.server_capabilities & 0xFFFF)  # lower capabilities flag
            + types.int_1(self.server_character_set)  # lower character set
            + types.int_2(self.status_flags)  # server status flag
            + types.int_2(self.server_capabilities >> 16)  # higher capabilities flag
            + types.int_1(0)  # constant 0 (no CLIENT_PLUGIN_AUTH)
            + types.str_fixed(10, bytes(10))  # reserved
            + types.str_fixed(13, bytes(13))  # rest of plugin data
        )

    def text_resultset(self, result_set):
        data = [b""]

        if types.Capabilities.CLIENT_OPTIONAL_RESULTSET_METADATA in self.capabilities:
            data[0] += types.int_1(types.ResultsetMetadata.RESULTSET_METADATA_FULL)

        data[0] += types.int_len(len(result_set.columns))

        for column in result_set.columns:
            data.append(
                self.column_definition_41(
                    name=column.name,
                    column_type=column.type,
                    character_set=column.character_set,
                )
            )

        if not types.Capabilities.CLIENT_DEPRECATE_EOF in self.capabilities:
            data.append(self.eof())

        for row in result_set.rows:
            row_data = []

            for value, column in zip(row, result_set.columns):
                if value is None:
                    row_data.append(b"\xfb")
                else:
                    row_data.append(types.str_len(column.encoder(value)))
            data.append(b"".join(row_data))

        if types.Capabilities.CLIENT_DEPRECATE_EOF in self.capabilities:
            data.append(self.ok(eof=True, affected_rows=len(result_set.rows)))
        else:
            data.append(self.eof())

        return data

    async def start(self):
        await self._connection_phase()

        session = self.session_factory()
        await session.init(self)

        try:
            await self._command_phase(session)
        finally:
            await session.close()

    async def _connection_phase(self):
        self.stream.write(self.handshake_v10())
        self.handshake_response_41(await self.stream.read())
        self.stream.write(self.ok())
        self.stream.reset_seq()

    async def _command_phase(self, session):
        while True:
            data = await self.stream.read()
            command = data[0]
            rest = data[1:]

            if command == types.Commands.COM_QUIT:
                return

            if command in (
                types.Commands.COM_PING,
                types.Commands.COM_RESET_CONNECTION,
                types.Commands.COM_DEBUG,
            ):
                self.stream.write(self.ok())
                self.stream.reset_seq()
                continue

            if command == types.Commands.COM_QUERY:
                # pylint: disable=broad-except
                try:
                    result_set = await session.query(rest.decode("utf-8"))

                    if result_set is None:
                        self.stream.write(self.ok())
                        self.stream.reset_seq()
                        continue

                    result_set = ensure_result_set(result_set)

                    for packet in self.text_resultset(result_set):
                        self.stream.write(packet)
                    self.stream.reset_seq()
                    continue

                except Exception as e:
                    logger.exception(e)
                    self.stream.write(self.error(e))
                    self.stream.reset_seq()
                    continue

            self.stream.write(
                self.error("Unknown Command", code=1047, sql_state=b"08S01")
            )
            self.stream.reset_seq()
