"""Basic data types of the mysql protocol"""

import struct

from enum import IntEnum, IntFlag, auto


class FieldTypes(IntEnum):
    MYSQL_TYPE_DECIMAL = auto()
    MYSQL_TYPE_TINY = auto()
    MYSQL_TYPE_SHORT = auto()
    MYSQL_TYPE_LONG = auto()
    MYSQL_TYPE_FLOAT = auto()
    MYSQL_TYPE_DOUBLE = auto()
    MYSQL_TYPE_NULL = auto()
    MYSQL_TYPE_TIMESTAMP = auto()
    MYSQL_TYPE_LONGLONG = auto()
    MYSQL_TYPE_INT24 = auto()
    MYSQL_TYPE_DATE = auto()
    MYSQL_TYPE_TIME = auto()
    MYSQL_TYPE_DATETIME = auto()
    MYSQL_TYPE_YEAR = auto()
    MYSQL_TYPE_NEWDATE = auto()
    MYSQL_TYPE_VARCHAR = auto()
    MYSQL_TYPE_BIT = auto()
    MYSQL_TYPE_TIMESTAMP2 = auto()
    MYSQL_TYPE_DATETIME2 = auto()
    MYSQL_TYPE_TIME2 = auto()
    MYSQL_TYPE_TYPED_ARRAY = auto()
    MYSQL_TYPE_INVALID = 243
    MYSQL_TYPE_BOOL = 244
    MYSQL_TYPE_JSON = 245
    MYSQL_TYPE_NEWDECIMAL = 246
    MYSQL_TYPE_ENUM = 247
    MYSQL_TYPE_SET = 248
    MYSQL_TYPE_TINY_BLOB = 249
    MYSQL_TYPE_MEDIUM_BLOB = 250
    MYSQL_TYPE_LONG_BLOB = 251
    MYSQL_TYPE_BLOB = 252
    MYSQL_TYPE_VAR_STRING = 253
    MYSQL_TYPE_STRING = 254
    MYSQL_TYPE_GEOMETRY = 255


class Commands(IntEnum):
    COM_QUIT = 1
    COM_INIT_DB = 2
    COM_QUERY = 3
    COM_FIELD_LIST = 4
    COM_REFRESH = 7
    COM_STATISTICS = 8
    COM_PROCESS_INFO = 10
    COM_PROCESS_KILL = 12
    COM_DEBUG = 13
    COM_PING = 14
    COM_CHANGE_USER = 17
    COM_SET_OPTION = 26
    COM_RESET_CONNECTION = 31


class ColumnDefinition(IntFlag):
    NOT_NULL_FLAG = auto()
    PRI_KEY_FLAG = auto()
    UNIQUE_KEY_FLAG = auto()
    MULTIPLE_KEY_FLAG = auto()
    BLOB_FLAG = auto()
    UNSIGNED_FLAG = auto()
    ZEROFILL_FLAG = auto()
    BINARY_FLAG = auto()
    ENUM_FLAG = auto()
    AUTO_INCREMENT_FLAG = auto()
    TIMESTAMP_FLAG = auto()
    SET_FLAG = auto()
    NO_DEFAULT_VALUE_FLAG = auto()
    ON_UPDATE_NOW_FLAG = auto()
    NUM_FLAG = auto()
    PART_KEY_FLAG = auto()
    GROUP_FLAG = auto()
    UNIQUE_FLAG = auto()
    BINCMP_FLAG = auto()
    GET_FIXED_FIELDS_FLAG = auto()
    FIELD_IN_PART_FUNC_FLAG = auto()
    FIELD_IN_ADD_INDEX = auto()
    FIELD_IS_RENAMED = auto()
    FIELD_FLAGS_STORAGE_MEDIA = auto()
    FIELD_FLAGS_STORAGE_MEDIA_MASK = auto()
    FIELD_FLAGS_COLUMN_FORMAT = auto()
    FIELD_FLAGS_COLUMN_FORMAT_MASK = auto()
    FIELD_IS_DROPPED = auto()
    EXPLICIT_NULL_FLAG = auto()
    NOT_SECONDARY_FLAG = auto()
    FIELD_IS_INVISIBLE = auto()


class Capabilities(IntFlag):
    CLIENT_LONG_PASSWORD = auto()
    CLIENT_FOUND_ROWS = auto()
    CLIENT_LONG_FLAG = auto()
    CLIENT_CONNECT_WITH_DB = auto()
    CLIENT_NO_SCHEMA = auto()
    CLIENT_COMPRESS = auto()
    CLIENT_ODBC = auto()
    CLIENT_LOCAL_FILES = auto()
    CLIENT_IGNORE_SPACE = auto()
    CLIENT_PROTOCOL_41 = auto()
    CLIENT_INTERACTIVE = auto()
    CLIENT_SSL = auto()
    CLIENT_IGNORE_SIGPIPE = auto()
    CLIENT_TRANSACTIONS = auto()
    CLIENT_RESERVED = auto()
    CLIENT_RESERVED2 = auto()
    CLIENT_MULTI_STATEMENTS = auto()
    CLIENT_MULTI_RESULTS = auto()
    CLIENT_PS_MULTI_RESULTS = auto()
    CLIENT_PLUGIN_AUTH = auto()
    CLIENT_CONNECT_ATTRS = auto()
    CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = auto()
    CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS = auto()
    CLIENT_SESSION_TRACK = auto()
    CLIENT_DEPRECATE_EOF = auto()
    CLIENT_OPTIONAL_RESULTSET_METADATA = auto()
    CLIENT_ZSTD_COMPRESSION_ALGORITHM = auto()
    CLIENT_QUERY_ATTRIBUTES = auto()
    MULTI_FACTOR_AUTHENTICATION = auto()
    CLIENT_CAPABILITY_EXTENSION = auto()
    CLIENT_SSL_VERIFY_SERVER_CERT = auto()
    CLIENT_REMEMBER_OPTIONS = auto()


class ServerStatus(IntFlag):
    SERVER_STATUS_IN_TRANS = auto()
    SERVER_STATUS_AUTOCOMMIT = auto()
    SERVER_STATUS_UNKNOWN = auto()
    SERVER_MORE_RESULTS_EXISTS = auto()
    SERVER_QUERY_NO_GOOD_INDEX_USED = auto()
    SERVER_QUERY_NO_INDEX_USES = auto()
    SERVER_STATUS_CURSOR_EXISTS = auto()
    SERVER_STATUS_LAST_ROW_SENT = auto()
    SERVER_STATUS_DB_DROPPED = auto()
    SERVER_STATUS_NO_BACKSLASH_ESCAPES = auto()
    SERVER_STATUS_METADATA_CHANGED = auto()
    SERVER_QUERY_WAS_SLOW = auto()
    SERVER_PS_OUT_PARAMS = auto()
    SERVER_STATUS_IN_TRANS_READONLY = auto()
    SERVER_SESSION_STATE_CHANGED = auto()


class ResultsetMetadata(IntEnum):
    RESULTSET_METADATA_NONE = 0
    RESULTSET_METADATA_FULL = 1


def int_len(i):
    if i < 251:
        return struct.pack("<B", i)
    if i < 2**16:
        return struct.pack("<BH", 0xFC, i)
    if i < 2**24:
        return struct.pack("<BL", 0xFD, i)[:-1]

    return struct.pack("<BQ", 0xFE, i)


def int_1(i):
    return struct.pack("<B", i)


def int_2(i):
    return struct.pack("<H", i)


def int_3(i):
    return struct.pack("<HB", i & 0xFFFF, i >> 16)


def int_4(i):
    return struct.pack("<I", i)


def int_6(i):
    return struct.pack("<IH", i & 0xFFFFFFFF, i >> 32)


def int_8(i):
    return struct.pack("<Q", i)


def str_fixed(l, s):
    return struct.pack(f"<{l}s", s)


def str_null(s):
    l = len(s)
    return struct.pack(f"<{l}sB", s, 0)


def str_len(s):
    l = len(s)
    return int_len(l) + str_fixed(l, s)


def str_rest(s):
    l = len(s)
    return str_fixed(l, s)


def read_int_1(reader):
    data = reader.read(1)
    return struct.unpack("<B", data)[0]


def read_int_2(reader):
    data = reader.read(2)
    return struct.unpack("<H", data)[0]


def read_int_3(reader):
    data = reader.read(3)
    t = struct.unpack("<HB", data)
    return t[0] + (t[1] << 16)


def read_int_4(reader):
    data = reader.read(4)
    return struct.unpack("<I", data)[0]


def read_int_6(reader):
    data = reader.read(6)
    t = struct.unpack("<IH", data)
    return t[0] + (t[1] << 32)


def read_int_8(reader):
    data = reader.read(8)
    return struct.unpack("<Q", data)[0]


def read_int_len(reader):
    i = read_int_1(reader)

    if i == 0xFE:
        return read_int_8(reader)

    if i == 0xFD:
        return read_int_3(reader)

    if i == 0xFC:
        return read_int_2(reader)

    return i


def read_str_fixed(reader, l):
    return reader.read(l)


def read_str_null(reader):
    data = b""
    while True:
        b = reader.read(1)
        if b == b"\x00":
            return data
        data += b


def read_str_len(reader):
    l = read_int_len(reader)
    return read_str_fixed(reader, l)


def read_str_rest(reader):
    return reader.read()
