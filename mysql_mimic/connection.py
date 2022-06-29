import io
import logging
import re
from dataclasses import dataclass
from typing import Optional, Dict, Iterable

from mysql_mimic.constants import DEFAULT_SERVER_CAPABILITIES
from mysql_mimic.errors import ErrorCode, MysqlError, get_sqlstate
from mysql_mimic.results import (
    ensure_result_set,
    text_encode,
    binary_encode,
    NullBitmap,
)
from mysql_mimic.charset import CharacterSet, Collation
from mysql_mimic import types
from mysql_mimic.utils import seq
from mysql_mimic.admin import Admin

logger = logging.getLogger(__name__)


# Borrowed from mysql-connector-python
REGEX_PARAM = re.compile(r"""\?(?=(?:[^"'`]*["'`][^"'`]*["'`])*[^"'`]*$)""")


class Connection:
    _MAX_PREPARED_STMT_ID = 2**32

    def __init__(
        self,
        stream,
        connection_id,
        session,
        server_capabilities=DEFAULT_SERVER_CAPABILITIES,
        force_cursor=False,
    ):
        """
        Client connection.

        Args:
            stream (mysql_mimic.stream.MysqlStream): stream to use for writing/reading
            connection_id (int): 32 bit connection ID
            session (mysql_mimic.session.Session): session
            server_capabilities (int): server capability flags
            force_cursor (bool): If True, always send a cursor when executing prepared statements,
                even if the client doesn't explicitly request it. This is here to get around a bug
                in mysql-connector-python, which doesn't properly set cursor flags.
        """
        self.stream = stream
        self.session = session
        self.connection_id = connection_id

        self.server_capabilities = server_capabilities
        self.client_capabilities = types.Capabilities(0)
        self.capabilities = types.Capabilities(0)
        self.status_flags = types.ServerStatus(0)

        self.max_packet_size = 0
        self.auth_response = None
        self.client_plugin_name = None
        self.client_connect_attrs = {}
        self.zstd_compression_level = 0
        self.force_cursor = force_cursor

        self.prepared_stmt_seq = seq(self._MAX_PREPARED_STMT_ID)
        self.prepared_stmts = {}

        self.admin = Admin(connection_id=connection_id, session=session)

    @property
    def server_character_set(self):
        return self.admin.server_character_set

    @property
    def client_character_set(self):
        return self.admin.client_character_set

    @property
    def database(self):
        return self.admin.database

    @property
    def username(self):
        return self.admin.username

    async def start(self):
        logger.info("Started new connection: %s", self.connection_id)
        try:
            await self.connection_phase()
            await self.session.init(self)
        except Exception as e:
            await self.stream.write(self.error(e, code=ErrorCode.HANDSHAKE_ERROR))
            raise

        try:
            await self.command_phase()
        finally:
            await self.session.close()

    async def connection_phase(self):
        """https://dev.mysql.com/doc/internals/en/connection-phase.html"""
        await self.stream.write(self.handshake_v10())
        self.handshake_response_41(await self.stream.read())
        await self.stream.write(self.ok())
        self.stream.reset_seq()

    async def command_phase(self):
        """https://dev.mysql.com/doc/internals/en/command-phase.html"""
        while True:
            data = await self.stream.read()
            try:
                command = data[0]
                rest = data[1:]

                if command == types.Commands.COM_QUERY:
                    await self.handle_query(rest)
                elif command == types.Commands.COM_STMT_PREPARE:
                    await self.handle_stmt_prepare(rest)
                elif command == types.Commands.COM_STMT_SEND_LONG_DATA:
                    await self.handle_stmt_send_long_data(rest)
                elif command == types.Commands.COM_STMT_EXECUTE:
                    await self.handle_stmt_execute(rest)
                elif command == types.Commands.COM_STMT_FETCH:
                    await self.handle_stmt_fetch(rest)
                elif command == types.Commands.COM_STMT_RESET:
                    await self.handle_stmt_reset(rest)
                elif command == types.Commands.COM_STMT_CLOSE:
                    await self.handle_stmt_close(rest)
                elif command == types.Commands.COM_PING:
                    await self.handle_ping(rest)
                elif command == types.Commands.COM_RESET_CONNECTION:
                    await self.handle_reset_connection(rest)
                elif command == types.Commands.COM_DEBUG:
                    await self.handle_debug(rest)
                elif command == types.Commands.COM_QUIT:
                    return
                else:
                    raise MysqlError(
                        f"Unsupported Command: {hex(command)}",
                        ErrorCode.UNKNOWN_COM_ERROR,
                    )

            except MysqlError as e:
                logger.exception(e)
                await self.stream.write(self.error(msg=e.msg, code=e.code))
            except Exception as e:  # pylint: disable=broad-except
                logger.exception(e)
                await self.stream.write(self.error(e))
            finally:
                self.stream.reset_seq()

    async def handle_ping(self, data):  # pylint: disable=unused-argument
        """
        https://dev.mysql.com/doc/internals/en/com-ping.html

        COM_PING check if the server is alive.
        """
        await self.stream.write(self.ok())

    async def handle_reset_connection(self, data):  # pylint: disable=unused-argument
        """
        https://dev.mysql.com/doc/internals/en/com-reset-connection.html

        COM_RESET_CONNECTION Resets the session state.

        For now, we're just treating this like a no-op.
        """
        await self.stream.write(self.ok())

    async def handle_debug(self, data):  # pylint: disable=unused-argument
        """
        https://dev.mysql.com/doc/internals/en/com-debug.html

        COM_DEBUG triggers a dump on internal debug info to stdout of the mysql-server.

        For now, we're just treating this like a no-op.
        """
        await self.stream.write(self.ok())

    async def handle_query(self, data):
        """
        https://dev.mysql.com/doc/internals/en/com-query.html

        COM_QUERY is used to send the server a text-based query that is executed immediately.
        """
        sql = data.decode(self.client_character_set.codec)

        result_set = await self.query(sql)

        if not result_set:
            await self.stream.write(self.ok())
            return

        for packet in self.text_resultset(result_set):
            await self.stream.write(packet)

    async def handle_stmt_prepare(self, data):
        """
        https://dev.mysql.com/doc/internals/en/com-stmt-prepare.html

        COM_STMT_PREPARE creates a prepared statement from the passed query string.
        """
        sql = data.decode(self.client_character_set.codec)

        stmt_id = next(self.prepared_stmt_seq)
        num_params = len(REGEX_PARAM.findall(sql))

        stmt = PreparedStatement(
            stmt_id=stmt_id,
            sql=sql,
            num_params=num_params,
        )
        self.prepared_stmts[stmt_id] = stmt

        for packet in self.stmt_prepare_ok(stmt):
            await self.stream.write(packet)

    async def handle_stmt_send_long_data(self, data):
        """
        https://dev.mysql.com/doc/internals/en/com-stmt-send-long-data.html

        COM_STMT_SEND_LONG_DATA sends the data for a column.
        """
        r = io.BytesIO(data)
        stmt_id = types.read_uint_4(r)
        param_id = types.read_uint_2(r)
        stmt = self.get_stmt(stmt_id)
        if stmt.param_buffers is None:
            stmt.param_buffers = {}
        buffer = stmt.param_buffers.setdefault(param_id, bytearray())
        buffer.extend(r.read())

    async def handle_stmt_execute(self, data):
        """
        https://dev.mysql.com/doc/internals/en/com-stmt-execute.html

        COM_STMT_EXECUTE asks the server to execute a prepared statement as identified by stmt-id.
        """
        r = io.BytesIO(data)
        stmt_id = types.read_uint_4(r)
        stmt = self.get_stmt(stmt_id)
        use_cursor = self.read_cursor_flags(r)
        types.read_uint_4(r)  # iteration count
        sql = self.interpolate_params(r, stmt)
        stmt.param_buffers = None

        result_set = await self.query(sql)

        if not result_set:
            await self.stream.write(self.ok())
            return

        await self.stream.write(types.uint_len(len(result_set.columns)))

        for column in result_set.columns:
            await self.stream.write(
                self.column_definition_41(
                    name=column.name,
                    column_type=column.type,
                    character_set=column.character_set,
                )
            )

        rows = (self.binary_resultrow(r) for r in result_set.rows)

        if use_cursor:
            stmt.cursor = rows
            await self.stream.write(
                self.ok_or_eof(flags=types.ServerStatus.SERVER_STATUS_CURSOR_EXISTS)
            )
        else:
            if not self.deprecate_eof():
                self.eof()
            for row in rows:
                await self.stream.write(row)
            await self.stream.write(self.ok_or_eof())

    async def handle_stmt_fetch(self, data):
        """
        https://dev.mysql.com/doc/internals/en/com-stmt-fetch.html

        COM_STMT_FETCH fetches rows from an existing resultset after a COM_STMT_EXECUTE.
        """
        r = io.BytesIO(data)
        stmt_id = types.read_uint_4(r)
        num_rows = types.read_uint_4(r)
        stmt = self.get_stmt(stmt_id)

        count = 0
        for _, packet in zip(range(num_rows), stmt.cursor):
            await self.stream.write(packet)
            count += 1

        done = count < num_rows

        await self.stream.write(
            self.ok_or_eof(
                flags=types.ServerStatus.SERVER_STATUS_LAST_ROW_SENT
                if done
                else types.ServerStatus.SERVER_STATUS_CURSOR_EXISTS
            )
        )

    async def handle_stmt_reset(self, data):
        """
        https://dev.mysql.com/doc/internals/en/com-stmt-reset.html

        COM_STMT_RESET resets the data of a prepared statement which was accumulated with COM_STMT_SEND_LONG_DATA
        commands and closes the cursor if it was opened with COM_STMT_EXECUTE.
        """
        r = io.BytesIO(data)
        stmt_id = types.read_uint_4(r)
        stmt = self.get_stmt(stmt_id)
        stmt.buffers = None
        stmt.cursor = None
        await self.stream.write(self.ok())

    async def handle_stmt_close(self, data):
        """
        https://dev.mysql.com/doc/internals/en/com-stmt-close.html

        COM_STMT_CLOSE deallocates a prepared statement.
        """
        r = io.BytesIO(data)
        stmt_id = types.read_uint_4(r)
        self.prepared_stmts.pop(stmt_id, None)

    def read_cursor_flags(self, reader):
        flags = types.read_uint_1(reader)
        if (
            self.force_cursor
            or flags == types.ComStmtExecuteFlags.CURSOR_TYPE_READ_ONLY
        ):
            return True
        if flags == types.ComStmtExecuteFlags.CURSOR_TYPE_NO_CURSOR:
            return False
        raise MysqlError(
            f"Unsupported cursor flags: {flags}", ErrorCode.NOT_SUPPORTED_YET
        )

    def interpolate_params(self, reader, stmt):
        sql = stmt.sql
        if stmt.num_params:
            null_bitmap = NullBitmap.from_buffer(reader, stmt.num_params)
            new_params_bound_flag = types.read_uint_1(reader)

            if not new_params_bound_flag:
                raise MysqlError(
                    "Server requires the new-params-bound-flag to be set",
                    ErrorCode.NOT_SUPPORTED_YET,
                )

            param_types = []
            for _ in range(stmt.num_params):
                param_type = types.ColumnType(types.read_uint_1(reader))
                is_unsigned = (types.read_uint_1(reader) & 0x80) > 0
                param_types.append((param_type, is_unsigned))

            for param_id, (param_type, is_unsigned) in enumerate(param_types):
                if null_bitmap.is_flipped(param_id):
                    param = "NULL"
                else:
                    param = self.read_param(
                        reader, param_type, is_unsigned, param_id, stmt
                    )
                sql = REGEX_PARAM.sub(param, sql, 1)

        return sql

    def read_param(self, reader, param_type, unsigned, param_id, stmt):
        if stmt.param_buffers and param_id in stmt.param_buffers:
            decoded = bytes(stmt.param_buffers[param_id]).decode(
                self.client_character_set.codec
            )
            return f"'{decoded}'"

        if param_type in {
            types.ColumnType.VARCHAR,
            types.ColumnType.VAR_STRING,
            types.ColumnType.STRING,
            types.ColumnType.BLOB,
            types.ColumnType.TINY_BLOB,
            types.ColumnType.MEDIUM_BLOB,
            types.ColumnType.LONG_BLOB,
        }:
            val = types.read_str_len(reader)
            decoded = val.decode(self.client_character_set.codec)
            return f"'{decoded}'"

        if param_type == types.ColumnType.TINY:
            return str((types.read_uint_1 if unsigned else types.read_int_1)(reader))

        if param_type == types.ColumnType.BOOL:
            return "TRUE" if types.read_uint_1(reader) else "FALSE"

        if param_type in {types.ColumnType.SHORT, types.ColumnType.YEAR}:
            return str((types.read_uint_2 if unsigned else types.read_int_2)(reader))

        if param_type in {types.ColumnType.LONG, types.ColumnType.INT24}:
            return str((types.read_uint_4 if unsigned else types.read_int_4)(reader))

        if param_type == types.ColumnType.LONGLONG:
            return str((types.read_uint_8 if unsigned else types.read_int_8)(reader))

        if param_type == types.ColumnType.FLOAT:
            return str(types.read_float(reader))

        if param_type == types.ColumnType.DOUBLE:
            return str(types.read_double(reader))

        if param_type == types.ColumnType.NULL:
            return "NULL"

        raise MysqlError(
            f"Unsupported parameter type: {param_type}", ErrorCode.NOT_SUPPORTED_YET
        )

    def get_stmt(self, stmt_id):
        if stmt_id in self.prepared_stmts:
            return self.prepared_stmts[stmt_id]
        raise MysqlError(f"Unknown statement: {stmt_id}", ErrorCode.UNKNOWN_PROCEDURE)

    async def query(self, sql):
        result_set = await self.admin.parse(sql)

        if result_set is None:
            sql = self.admin.replace_variables(sql)
            result_set = await self.session.query(sql)
            result_set = result_set and ensure_result_set(result_set)

        return result_set

    def ok(self, eof=False, affected_rows=0, last_insert_id=0, warnings=0, flags=0):
        """https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html"""
        data = types.uint_1(0) if not eof else types.uint_1(0xFE)
        data += types.uint_len(affected_rows) + types.uint_len(last_insert_id)

        if types.Capabilities.CLIENT_PROTOCOL_41 in self.capabilities:
            data += types.uint_2(self.status_flags | flags)
            data += types.uint_2(warnings)
        elif types.Capabilities.CLIENT_TRANSACTIONS in self.capabilities:
            data += types.uint_2(self.status_flags | flags)

        return data

    def eof(self, warnings=0, flags=0):
        """https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html"""
        data = types.uint_1(0xFE)

        if types.Capabilities.CLIENT_PROTOCOL_41 in self.capabilities:
            data += types.uint_2(warnings)
            data += types.uint_2(self.status_flags | flags)

        return data

    def ok_or_eof(self, affected_rows=0, last_insert_id=0, warnings=0, flags=0):
        if self.deprecate_eof():
            return self.ok(
                eof=True,
                affected_rows=affected_rows,
                last_insert_id=last_insert_id,
                warnings=warnings,
                flags=flags,
            )
        return self.eof(warnings, flags)

    def error(
        self,
        msg,
        code=ErrorCode.UNKNOWN_ERROR,
    ):
        """https://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html"""
        data = types.uint_1(0xFF) + types.uint_2(code)

        if types.Capabilities.CLIENT_PROTOCOL_41 in self.capabilities:
            data += types.str_fixed(1, b"#")
            data += types.str_fixed(5, get_sqlstate(code))

        data += types.str_rest(str(msg).encode(self.server_character_set.codec))

        return data

    # pylint: disable=too-many-arguments
    def column_definition_41(
        self,
        schema=None,
        table=None,
        org_table=None,
        name=None,
        org_name=None,
        character_set=CharacterSet.utf8mb4,
        column_length=256,
        column_type=types.ColumnType.VARCHAR,
        flags=types.ColumnDefinition(0),
        decimals=0,
    ):
        """https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition41"""
        schema = schema or ""
        table = table or ""
        org_table = org_table or table
        name = name or ""
        org_name = org_name or name

        return (
            types.str_len(b"def")
            + types.str_len(schema.encode(self.server_character_set.codec))
            + types.str_len(table.encode(self.server_character_set.codec))
            + types.str_len(org_table.encode(self.server_character_set.codec))
            + types.str_len(name.encode(self.server_character_set.codec))
            + types.str_len(org_name.encode(self.server_character_set.codec))
            + types.uint_len(0x0C)  # Length of the following fields
            + types.uint_2(character_set)
            + types.uint_4(column_length)
            + types.uint_1(column_type)
            + types.uint_2(flags)
            + types.uint_1(decimals)
            + types.uint_2(0)  # filler
        )

    def handshake_response_41(self, data):
        """https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse41"""
        r = io.BytesIO(data)
        self.client_capabilities = types.Capabilities(types.read_uint_4(r))
        self.capabilities = self.server_capabilities & self.client_capabilities
        self.max_packet_size = types.read_uint_4(r)
        self.admin.client_character_set = Collation(types.read_uint_1(r)).charset
        types.read_str_fixed(r, 23)
        self.admin.username = types.read_str_null(r).decode()

        if (
            types.Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
            in self.capabilities
        ):
            self.auth_response = types.read_str_len(r).decode()
        else:
            l_auth_response = types.read_uint_1(r)
            self.auth_response = types.read_str_fixed(r, l_auth_response).decode()

        if types.Capabilities.CLIENT_CONNECT_WITH_DB in self.capabilities:
            self.admin.database = types.read_str_null(r).decode()

        if types.Capabilities.CLIENT_PLUGIN_AUTH in self.capabilities:
            self.client_plugin_name = types.read_str_null(r).decode()

        if types.Capabilities.CLIENT_CONNECT_ATTRS in self.capabilities:
            total_l = types.read_uint_len(r)

            while total_l > 0:
                key = types.read_str_len(r)
                value = types.read_str_len(r)
                self.client_connect_attrs[key.decode()] = value.decode()

                item_l = len(types.str_len(key) + types.str_len(value))
                total_l -= item_l

        if types.Capabilities.CLIENT_ZSTD_COMPRESSION_ALGORITHM in self.capabilities:
            self.zstd_compression_level = types.read_uint_1(r)

    def handshake_v10(self):
        """https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeV10"""
        return (
            types.uint_1(10)  # Always 10
            + types.str_null(
                self.admin.mysql_version.encode(self.server_character_set.codec)
            )  # Status
            + types.uint_4(self.connection_id)  # Connection ID
            + types.str_null(bytes(8))  # plugin data
            + types.uint_2(self.server_capabilities & 0xFFFF)  # lower capabilities flag
            + types.uint_1(self.server_character_set)  # lower character set
            + types.uint_2(self.status_flags)  # server status flag
            + types.uint_2(self.server_capabilities >> 16)  # higher capabilities flag
            + types.uint_1(0)  # constant 0 (no CLIENT_PLUGIN_AUTH)
            + types.str_fixed(10, bytes(10))  # reserved
            + types.str_fixed(13, bytes(13))  # rest of plugin data
        )

    def deprecate_eof(self):
        return types.Capabilities.CLIENT_DEPRECATE_EOF in self.capabilities

    def text_resultset(self, result_set):
        """https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset"""
        column_count = b""

        if types.Capabilities.CLIENT_OPTIONAL_RESULTSET_METADATA in self.capabilities:
            column_count += types.uint_1(
                types.ResultsetMetadata.RESULTSET_METADATA_FULL
            )

        column_count += types.uint_len(len(result_set.columns))

        packets = [column_count]

        for column in result_set.columns:
            packets.append(
                self.column_definition_41(
                    name=column.name,
                    column_type=column.type,
                    character_set=column.character_set,
                )
            )

        if not self.deprecate_eof():
            packets.append(self.eof())

        affected_rows = 0

        for row in result_set.rows:
            affected_rows += 1
            row_data = []

            for value, column in zip(row, result_set.columns):
                if value is None:
                    row_data.append(b"\xfb")
                else:
                    text = text_encode(value)
                    if isinstance(text, str):
                        text = text.encode(self.server_character_set.codec)
                    row_data.append(types.str_len(text))
            packets.append(b"".join(row_data))

        packets.append(self.ok_or_eof(affected_rows=affected_rows))

        return packets

    def binary_resultrow(self, row):
        """https://dev.mysql.com/doc/internals/en/binary-protocol-resultset-row.html"""
        column_count = len(row)

        null_bitmap = NullBitmap.new(column_count, offset=2)

        values = []
        for i, val in enumerate(row):
            if val is None:
                null_bitmap.flip(i)
            else:
                values.append(binary_encode(val))

        values_data = b"".join(values)

        null_bitmap_data = bytes(null_bitmap)

        return b"".join(
            [
                types.uint_1(0),  # packet header
                types.str_fixed(len(null_bitmap_data), null_bitmap_data),
                types.str_fixed(len(values_data), values_data),
            ]
        )

    def stmt_prepare_ok(self, statement):
        """https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK"""
        num_params = statement.num_params

        packets = [
            b"".join(
                [
                    types.uint_1(0),  # OK
                    types.uint_4(statement.stmt_id),
                    types.uint_2(0),  # number of columns
                    types.uint_2(num_params),
                    types.uint_1(0),  # filler
                    types.uint_2(0),  # number of warnings
                ]
            )
        ]

        if num_params:
            for _ in range(num_params):
                packets.append(self.column_definition_41(name="?"))
            packets.append(self.eof())

        return packets


@dataclass
class PreparedStatement:
    stmt_id: int
    sql: str
    num_params: int
    param_buffers: Optional[Dict[int, bytearray]] = None
    cursor: Optional[Iterable] = None
