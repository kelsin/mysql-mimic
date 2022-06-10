import asyncio
import io
import struct

import pandas

from mysql_mimic import types
from mysql_mimic.version import __version__ as VERSION
from mysql_mimic.constants import DEFAULT_SERVER_CAPABILITIES


class seq:
    def __init__(self):
        self.num = 0

    def get(self):
        current = self.num
        self.num += 1
        return current


class MysqlStream:
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self._seq = seq()

    def seq(self):
        return self._seq.get()

    async def read(self):
        data = b""
        while True:
            i = struct.unpack("<I", await self.reader.read(4))[0]
            l = i & 0x00FFFFFF
            s = (i & 0xFF000000) >> 24

            expected = self.seq()
            if s != expected:
                raise ValueError(f"Expected seq({expected}) got seq({s})")

            if l == 0:
                return data

            data += await self.reader.read(l)

            if l < 0xFFFFFF:
                return data

    def write(self, data):
        while True:
            # Grab first 0xFFFFFF bytes to send
            send = data[:0xFFFFFF]
            data = data[0xFFFFFF:]

            i = len(send) + (self.seq() << 24)
            header = struct.pack("<I", i)
            self.writer.write(header + send)

            # We are done unless len(send) == 0xFFFFFF
            if len(send) != 0xFFFFFF:
                return

    def reset_seq(self):
        self._seq = seq()


# pylint: disable=unused-argument
def default_query_handler(query):
    if query.lower() == "select @@version_comment limit 1":
        return pandas.DataFrame(
            data={"@@version_comment": ["MySql-Mimic Python Proxy - MIT"]}
        )

    return pandas.DataFrame(data={"col1": ["foo", "bar"], "col2": [1.0, 2.0]})


class MysqlServer:
    def __init__(
        self, handler=default_query_handler, socket=None, host=None, port=3306
    ):
        self.handler = handler
        self.socket = socket
        self.host = host
        self.port = port
        self.server_capabilities = DEFAULT_SERVER_CAPABILITIES
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
        character_set=33,
        column_length=256,
        column_type=types.FieldTypes.MYSQL_TYPE_VARCHAR,
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
        self.username = types.read_str_null(r)

        if (
            types.Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
            in self.capabilities
        ):
            self.auth_response = types.read_str_len(r)
        else:
            l_auth_response = types.read_int_1(r)
            self.auth_response = types.read_str_fixed(r, l_auth_response)

        if types.Capabilities.CLIENT_CONNECT_WITH_DB in self.capabilities:
            self.database = types.read_str_null(r)

        if types.Capabilities.CLIENT_PLUGIN_AUTH in self.capabilities:
            self.client_plugin_name = types.read_str_null(r)

        if types.Capabilities.CLIENT_CONNECT_ATTRS in self.capabilities:
            total_l = types.read_int_len(r)

            while total_l > 0:
                key = types.read_str_len(r)
                value = types.read_str_len(r)
                self.client_connect_attrs[key] = value

                item_l = len(types.str_len(key) + types.str_len(value))
                total_l -= item_l

        if types.Capabilities.CLIENT_ZSTD_COMPRESSION_ALGORITHM in self.capabilities:
            self.zstd_compression_level = types.read_int_1(r)

    def handshake_v10(self):
        return (
            types.int_1(10)  # Always 10
            + types.str_null(VERSION.encode("utf-8"))  # Status
            + types.int_4(0)  # Connection ID
            + types.str_null(bytes(8))  # plugin data
            + types.int_2(self.server_capabilities & 0xFFFF)  # lower capabilities flag
            + types.int_1(self.server_character_set)  # lower character set
            + types.int_2(self.status_flags)  # server status flag
            + types.int_2(self.server_capabilities >> 16)  # higher capabilities flag
            + types.int_1(0)  # constant 0 (no CLIENT_PLUGIN_AUTH)
            + types.str_fixed(10, bytes(10))  # reserved
            + types.str_fixed(13, bytes(13))  # rest of plugin data
        )

    def text_resultset(self, df):
        data = [b""]

        if types.Capabilities.CLIENT_OPTIONAL_RESULTSET_METADATA in self.capabilities:
            data[0] += types.int_1(types.ResultsetMetadata.RESULTSET_METADATA_FULL)

        data[0] += types.int_len(len(df.columns))

        for column in df.columns:
            data.append(self.column_definition_41(name=column))

        if not types.Capabilities.CLIENT_DEPRECATE_EOF in self.capabilities:
            data.append(self.eof())

        for _, row in df.iterrows():
            row_data = b""
            for value in row:
                if not value:
                    row_data += b"0xFB"
                else:
                    row_data += types.str_len(str(value).encode("utf-8"))
            data.append(row_data)

        if types.Capabilities.CLIENT_DEPRECATE_EOF in self.capabilities:
            data.append(self.ok(eof=True, affected_rows=len(df)))
        else:
            data.append(self.eof())

        return data

    async def start(self, **kwargs):
        async def cb(reader, writer):
            s = MysqlStream(reader, writer)

            # Connection Phase
            s.write(self.handshake_v10())
            self.handshake_response_41(await s.read())
            s.write(self.ok())
            s.reset_seq()

            # Command Phase
            while True:
                data = await s.read()
                command = data[0]
                rest = data[1:]

                if command == types.Commands.COM_QUIT:
                    return

                if command in (
                    types.Commands.COM_PING,
                    types.Commands.COM_RESET_CONNECTION,
                    types.Commands.COM_DEBUG,
                ):
                    s.write(self.ok())
                    s.reset_seq()
                    continue

                if command == types.Commands.COM_QUERY:
                    # pylint: disable=broad-except
                    try:
                        result = self.handler(rest.decode("utf-8"))

                        if result is None:
                            s.write(self.ok())
                            s.reset_seq()
                            continue

                        if isinstance(result, pandas.DataFrame):
                            for packet in self.text_resultset(result):
                                s.write(packet)
                            s.reset_seq()
                            continue

                        s.write(self.ok())
                        s.reset_seq()
                        continue

                    except Exception as e:
                        s.write(self.error(e))
                        s.reset_seq()
                        continue

                s.write(self.error("Unknown Command", code=1047, sql_state=b"08S01"))
                s.reset_seq()

        if self.socket:
            server = await asyncio.start_unix_server(cb, path=self.socket, **kwargs)
        else:
            server = await asyncio.start_server(
                cb, host=self.host, port=self.port, **kwargs
            )

        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    mysql_server = MysqlServer()
    asyncio.run(mysql_server.start())
