import io
from dataclasses import dataclass, field
from typing import Optional, Dict

from mysql_mimic.charset import Collation, CharacterSet
from mysql_mimic.errors import ErrorCode, get_sqlstate, MysqlError
from mysql_mimic.prepared import PreparedStatement, REGEX_PARAM
from mysql_mimic.results import NullBitmap
from mysql_mimic.types import (
    Capabilities,
    uint_2,
    uint_1,
    str_null,
    uint_4,
    str_fixed,
    read_uint_4,
    read_str_null,
    read_str_fixed,
    read_uint_1,
    read_str_len,
    read_uint_len,
    str_len,
    str_rest,
    uint_len,
    ColumnType,
    read_int_1,
    read_int_2,
    read_uint_2,
    read_int_4,
    read_uint_8,
    read_int_8,
    read_float,
    read_double,
    ResultsetMetadata,
    ColumnDefinition,
    ComStmtExecuteFlags,
    peek,
)


@dataclass
class HandshakeResponse41:
    max_packet_size: int
    capabilities: Capabilities
    client_charset: CharacterSet
    username: str
    auth_response: bytes
    connect_attrs: Dict[str, str] = field(default_factory=dict)
    database: Optional[str] = None
    client_plugin: Optional[str] = None
    zstd_compression_level: int = 0


@dataclass
class ComChangeUser:
    username: str
    auth_response: bytes
    database: str
    client_charset: Optional[CharacterSet] = None
    client_plugin: Optional[str] = None
    connect_attrs: Dict[str, str] = field(default_factory=dict)


@dataclass
class ComQuery:
    sql: str
    query_attrs: Dict[str, str]


@dataclass
class ComStmtSendLongData:
    stmt_id: int
    param_id: int
    data: bytes


@dataclass
class ComStmtExecute:
    sql: str
    query_attrs: Dict[str, str]
    stmt: PreparedStatement
    use_cursor: bool


@dataclass
class ComStmtFetch:
    stmt_id: int
    num_rows: int


@dataclass
class ComStmtReset:
    stmt_id: int


@dataclass
class ComStmtClose:
    stmt_id: int


def make_ok(
    capabilities,
    status_flags,
    eof=False,
    affected_rows=0,
    last_insert_id=0,
    warnings=0,
    flags=0,
):
    parts = [
        uint_1(0) if not eof else uint_1(0xFE),
        uint_len(affected_rows),
        uint_len(last_insert_id),
    ]

    if Capabilities.CLIENT_PROTOCOL_41 in capabilities:
        parts.append(uint_2(status_flags | flags))
        parts.append(uint_2(warnings))
    elif Capabilities.CLIENT_TRANSACTIONS in capabilities:
        parts.append(uint_2(status_flags | flags))

    return _concat(*parts)


def make_eof(capabilities, status_flags, warnings=0, flags=0):
    parts = [uint_1(0xFE)]

    if Capabilities.CLIENT_PROTOCOL_41 in capabilities:
        parts.append(uint_2(warnings))
        parts.append(uint_2(status_flags | flags))

    return _concat(*parts)


def make_error(
    capabilities,
    server_charset,
    msg,
    code=ErrorCode.UNKNOWN_ERROR,
):
    parts = [uint_1(0xFF), uint_2(code)]

    if Capabilities.CLIENT_PROTOCOL_41 in capabilities:
        parts.append(str_fixed(1, b"#"))
        parts.append(str_fixed(5, get_sqlstate(code)))

    parts.append(str_rest(server_charset.encode(str(msg))))

    return _concat(*parts)


def make_handshake_v10(
    capabilities,
    server_charset,
    server_version,
    connection_id,
    auth_data,
    status_flags,
    auth_plugin_name,
):
    auth_plugin_data_len = (
        len(auth_data) if Capabilities.CLIENT_PLUGIN_AUTH in capabilities else 0
    )

    parts = [
        uint_1(10),  # protocol version
        str_null(server_charset.encode(server_version)),  # server version
        uint_4(connection_id),  # connection ID
        str_null(auth_data[:8]),  # plugin data
        uint_2(capabilities & 0xFFFF),  # lower capabilities flag
        uint_1(server_charset),  # lower character set
        uint_2(status_flags),  # server status flag
        uint_2(capabilities >> 16),  # higher capabilities flag
        uint_1(auth_plugin_data_len),
        str_fixed(10, bytes(10)),  # reserved
        str_fixed(max(13, auth_plugin_data_len - 8), auth_data[8:]),
    ]
    if Capabilities.CLIENT_PLUGIN_AUTH in capabilities:
        parts.append(str_null(server_charset.encode(auth_plugin_name)))
    return _concat(*parts)


def parse_handshake_response_41(capabilities, data):
    r = io.BytesIO(data)

    client_capabilities = Capabilities(read_uint_4(r))

    capabilities = capabilities & client_capabilities

    max_packet_size = read_uint_4(r)
    client_charset = Collation(read_uint_1(r)).charset
    read_str_fixed(r, 23)
    username = client_charset.decode(read_str_null(r))

    if Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA in capabilities:
        auth_response = read_str_len(r)
    else:
        l_auth_response = read_uint_1(r)
        auth_response = read_str_fixed(r, l_auth_response)

    response = HandshakeResponse41(
        max_packet_size=max_packet_size,
        capabilities=capabilities,
        client_charset=client_charset,
        username=username,
        auth_response=auth_response,
    )

    if Capabilities.CLIENT_CONNECT_WITH_DB in capabilities:
        response.database = client_charset.decode(read_str_null(r))

    if Capabilities.CLIENT_PLUGIN_AUTH in capabilities:
        response.client_plugin = client_charset.decode(read_str_null(r))

    if Capabilities.CLIENT_CONNECT_ATTRS in capabilities:
        response.connect_attrs = _read_connect_attrs(r, client_charset)

    if Capabilities.CLIENT_ZSTD_COMPRESSION_ALGORITHM in capabilities:
        response.zstd_compression_level = read_uint_1(r)

    return response


def make_auth_more_data(data):
    return _concat(uint_1(1), str_rest(data))  # status tag


def make_auth_switch_request(server_charset, plugin_name, plugin_provided_data):
    return _concat(
        uint_1(254),  # status tag
        str_null(server_charset.encode(plugin_name)),
        str_rest(plugin_provided_data),
    )


def parse_com_change_user(capabilities, client_charset, data):
    r = io.BytesIO(data)
    username = client_charset.decode(read_str_null(r))
    if Capabilities.CLIENT_SECURE_CONNECTION in capabilities:
        l_auth_response = read_uint_1(r)
        auth_response = read_str_fixed(r, l_auth_response)
    else:
        auth_response = read_str_null(r)
    database = client_charset.decode(read_str_null(r))

    response = ComChangeUser(
        username=username, auth_response=auth_response, database=database
    )

    if peek(r):  # more data available
        if Capabilities.CLIENT_PROTOCOL_41 in capabilities:
            client_charset = Collation(read_uint_2(r)).charset
            response.client_charset = client_charset
        if Capabilities.CLIENT_PLUGIN_AUTH in capabilities:
            response.client_plugin = client_charset.decode(read_str_null(r))
        if Capabilities.CLIENT_CONNECT_ATTRS in capabilities:
            response.connect_attrs = _read_connect_attrs(r, client_charset)

    return response


def parse_com_query(capabilities, client_charset, data):
    r = io.BytesIO(data)

    if Capabilities.CLIENT_QUERY_ATTRIBUTES in capabilities:
        parameter_count = read_uint_len(r)
        read_uint_len(r)  # parameter_set_count. Always 1.
        query_attrs = dict(
            _read_params(capabilities, client_charset, r, parameter_count)
        )
    else:
        query_attrs = {}

    sql = r.read().decode(client_charset.codec)

    return ComQuery(
        sql=sql,
        query_attrs=query_attrs,
    )


def make_column_count(capabilities, column_count):
    parts = []

    if Capabilities.CLIENT_OPTIONAL_RESULTSET_METADATA in capabilities:
        parts.append(uint_1(ResultsetMetadata.RESULTSET_METADATA_FULL))

    parts.append(uint_len(column_count))

    return _concat(*parts)


# pylint: disable=too-many-arguments
def make_column_definition_41(
    server_charset,
    schema=None,
    table=None,
    org_table=None,
    name=None,
    org_name=None,
    character_set=CharacterSet.utf8mb4,
    column_length=256,
    column_type=ColumnType.VARCHAR,
    flags=ColumnDefinition(0),
    decimals=0,
):
    schema = schema or ""
    table = table or ""
    org_table = org_table or table
    name = name or ""
    org_name = org_name or name

    return (
        str_len(b"def")
        + str_len(server_charset.encode(schema))
        + str_len(server_charset.encode(table))
        + str_len(server_charset.encode(org_table))
        + str_len(server_charset.encode(name))
        + str_len(server_charset.encode(org_name))
        + uint_len(0x0C)  # Length of the following fields
        + uint_2(character_set)
        + uint_4(column_length)
        + uint_1(column_type)
        + uint_2(flags)
        + uint_1(decimals)
        + uint_2(0)  # filler
    )


def make_text_resultset_row(row, columns):
    parts = []

    for value, column in zip(row, columns):
        if value is None:
            parts.append(b"\xfb")
        else:
            text = column.text_encode(value)
            parts.append(str_len(text))

    return _concat(*parts)


def make_com_stmt_prepare_ok(statement):
    """

    Args:
        statement (mysql_mimic.prepared.PreparedStatement)

    Returns:

    """
    return _concat(
        uint_1(0),  # OK
        uint_4(statement.stmt_id),
        uint_2(0),  # number of columns
        uint_2(statement.num_params),
        uint_1(0),  # filler
        uint_2(0),  # number of warnings
    )


def parse_com_stmt_send_long_data(data):
    r = io.BytesIO(data)
    return ComStmtSendLongData(
        stmt_id=read_uint_4(r),
        param_id=read_uint_2(r),
        data=r.read(),
    )


def parse_com_stmt_execute(capabilities, client_charset, data, get_stmt):
    r = io.BytesIO(data)
    stmt_id = read_uint_4(r)
    stmt = get_stmt(stmt_id)
    use_cursor, param_count_available = _read_cursor_flags(r)
    read_uint_4(r)  # iteration count. Always 1.
    sql, query_attrs = _interpolate_params(
        capabilities, client_charset, r, stmt, param_count_available
    )
    return ComStmtExecute(
        sql=sql,
        query_attrs=query_attrs,
        stmt=stmt,
        use_cursor=use_cursor,
    )


def make_binary_resultrow(row, columns):
    column_count = len(row)

    null_bitmap = NullBitmap.new(column_count, offset=2)

    values = []
    for i, (val, col) in enumerate(zip(row, columns)):
        if val is None:
            null_bitmap.flip(i)
        else:
            values.append(col.binary_encode(val))

    values_data = b"".join(values)

    null_bitmap_data = bytes(null_bitmap)

    return b"".join(
        [
            uint_1(0),  # packet header
            str_fixed(len(null_bitmap_data), null_bitmap_data),
            str_fixed(len(values_data), values_data),
        ]
    )


def parse_handle_stmt_fetch(data):
    r = io.BytesIO(data)
    return ComStmtFetch(
        stmt_id=read_uint_4(r),
        num_rows=read_uint_4(r),
    )


def parse_com_stmt_reset(data):
    r = io.BytesIO(data)
    return ComStmtReset(stmt_id=read_uint_4(r))


def parse_com_stmt_close(data):
    r = io.BytesIO(data)
    return ComStmtClose(stmt_id=read_uint_4(r))


def _read_cursor_flags(reader):
    flags = ComStmtExecuteFlags(read_uint_1(reader))
    param_count_available = ComStmtExecuteFlags.PARAMETER_COUNT_AVAILABLE in flags

    if ComStmtExecuteFlags.CURSOR_TYPE_READ_ONLY in flags:
        return True, param_count_available
    if ComStmtExecuteFlags.CURSOR_TYPE_NO_CURSOR in flags:
        return False, param_count_available
    raise MysqlError(f"Unsupported cursor flags: {flags}", ErrorCode.NOT_SUPPORTED_YET)


def _interpolate_params(
    capabilities, client_charset, reader, stmt, param_count_available
):
    sql = stmt.sql
    query_attrs = {}
    parameter_count = stmt.num_params

    if stmt.num_params > 0 or (
        Capabilities.CLIENT_QUERY_ATTRIBUTES in capabilities and param_count_available
    ):
        if Capabilities.CLIENT_QUERY_ATTRIBUTES in capabilities:
            parameter_count = read_uint_len(reader)

    if parameter_count > 0:
        # When there are query attributes, they are combined with statement parameters.
        # The statement parameters will be first, query attributes second.
        params = _read_params(
            capabilities, client_charset, reader, parameter_count, stmt.param_buffers
        )

        for _, value in params[: stmt.num_params]:
            sql = REGEX_PARAM.sub(_encode_param_as_sql(value), sql, 1)

        query_attrs = dict(params[stmt.num_params :])

    return sql, query_attrs


def _encode_param_as_sql(param):
    if isinstance(param, str):
        return f"'{param}'"
    if param is None:
        return "NULL"
    if param is True:
        return "TRUE"
    if param is False:
        return "FALSE"
    return str(param)


def _read_params(capabilities, client_charset, reader, parameter_count, buffers=None):
    """
    Read parameters from a stream.

    This is intended for reading query attributes from COM_QUERY and parameters from COM_STMT_EXECUTE.
    Args:
        capabilities (Capabilities)
        client_charset (CharacterSet)
        reader (io.BytesIO)
        parameter_count (int)
        buffers (dict[int, bytearray])
    Returns:
        list[tuple]: Name/value pairs. Name is None if there is no name, which is the case
            for stmt_execute, which combines statement parameters and query attributes.
    """
    params = []

    if parameter_count:
        null_bitmap = NullBitmap.from_buffer(reader, parameter_count)
        new_params_bound_flag = read_uint_1(reader)

        if not new_params_bound_flag:
            raise MysqlError(
                "Server requires the new-params-bound-flag to be set",
                ErrorCode.NOT_SUPPORTED_YET,
            )

        param_types = []

        for i in range(parameter_count):
            param_type, unsigned = _read_param_type(reader)

            if Capabilities.CLIENT_QUERY_ATTRIBUTES in capabilities:
                # Only query attributes have names
                # Statement parameters will have an empty name, e.g. b"\x00"
                param_name = client_charset.decode(read_str_len(reader))
            else:
                param_name = ""

            param_types.append((param_name, param_type, unsigned))

        for i, (param_name, param_type, unsigned) in enumerate(param_types):
            if null_bitmap.is_flipped(i):
                params.append((param_name, None))
            elif buffers and i in buffers:
                params.append((param_name, client_charset.decode(buffers[i])))
            else:
                params.append(
                    (
                        param_name,
                        _read_param_value(client_charset, reader, param_type, unsigned),
                    )
                )

    return params


def _read_param_type(reader):
    """
    Read a parameter type from a stream.

    Args:
        reader (io.BytesIO)
    Returns:
        tuple[types.ColumnType, bool]: tuple of (type, unsigned)
    """
    param_type = ColumnType(read_uint_1(reader))
    is_unsigned = (read_uint_1(reader) & 0x80) > 0
    return param_type, is_unsigned


def _read_param_value(client_charset, reader, param_type, unsigned):
    """
    Read a parameter value from a stream.

    Args:
        client_charset (CharacterSet)
        reader (io.BytesIO)
        param_type (types.ColumnType)
        unsigned (bool)
    Returns:
        parameter value
    """
    if param_type in {
        ColumnType.VARCHAR,
        ColumnType.VAR_STRING,
        ColumnType.STRING,
        ColumnType.BLOB,
        ColumnType.TINY_BLOB,
        ColumnType.MEDIUM_BLOB,
        ColumnType.LONG_BLOB,
    }:
        val = read_str_len(reader)
        return client_charset.decode(val)

    if param_type == ColumnType.TINY:
        return (read_uint_1 if unsigned else read_int_1)(reader)

    if param_type == ColumnType.BOOL:
        return read_uint_1(reader)

    if param_type in {ColumnType.SHORT, ColumnType.YEAR}:
        return (read_uint_2 if unsigned else read_int_2)(reader)

    if param_type in {ColumnType.LONG, ColumnType.INT24}:
        return (read_uint_4 if unsigned else read_int_4)(reader)

    if param_type == ColumnType.LONGLONG:
        return (read_uint_8 if unsigned else read_int_8)(reader)

    if param_type == ColumnType.FLOAT:
        return read_float(reader)

    if param_type == ColumnType.DOUBLE:
        return read_double(reader)

    if param_type == ColumnType.NULL:
        return None

    raise MysqlError(
        f"Unsupported parameter type: {param_type}", ErrorCode.NOT_SUPPORTED_YET
    )


def _read_connect_attrs(reader, client_charset):
    connect_attrs = {}
    total_l = read_uint_len(reader)

    while total_l > 0:
        key = read_str_len(reader)
        value = read_str_len(reader)
        connect_attrs[client_charset.decode(key)] = client_charset.decode(value)

        item_l = len(str_len(key) + str_len(value))
        total_l -= item_l
    return connect_attrs


def _concat(*parts):
    return b"".join(parts)
