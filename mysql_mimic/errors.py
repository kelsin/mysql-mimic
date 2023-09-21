from enum import IntEnum


class ErrorCode(IntEnum):
    """https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html"""

    CON_COUNT_ERROR = 1040
    HANDSHAKE_ERROR = 1043
    ACCESS_DENIED_ERROR = 1045
    NO_DB_ERROR = 1046
    PARSE_ERROR = 1064
    EMPTY_QUERY = 1065
    UNKNOWN_PROCEDURE = 1106
    UNKNOWN_SYSTEM_VARIABLE = 1193
    UNKNOWN_COM_ERROR = 1047
    UNKNOWN_ERROR = 1105
    WRONG_VALUE_FOR_VAR = 1231
    NOT_SUPPORTED_YET = 1235
    MALFORMED_PACKET = 1835
    USER_DOES_NOT_EXIST = 3162
    SESSION_WAS_KILLED = 3169
    PLUGIN_REQUIRES_REGISTRATION = 4055


# For more info, see https://dev.mysql.com/doc/refman/8.0/en/error-message-elements.html
SQLSTATES = {
    ErrorCode.CON_COUNT_ERROR: b"08004",
    ErrorCode.ACCESS_DENIED_ERROR: b"28000",
    ErrorCode.HANDSHAKE_ERROR: b"08S01",
    ErrorCode.NO_DB_ERROR: b"3D000",
    ErrorCode.PARSE_ERROR: b"42000",
    ErrorCode.EMPTY_QUERY: b"42000",
    ErrorCode.UNKNOWN_PROCEDURE: b"42000",
    ErrorCode.UNKNOWN_COM_ERROR: b"08S01",
    ErrorCode.WRONG_VALUE_FOR_VAR: b"42000",
    ErrorCode.NOT_SUPPORTED_YET: b"42000",
}


def get_sqlstate(code: ErrorCode) -> bytes:
    return SQLSTATES.get(code, b"HY000")


class MysqlError(Exception):
    def __init__(self, msg: str, code: ErrorCode = ErrorCode.UNKNOWN_ERROR):
        super().__init__(f"{code}: {msg}")
        self.msg = msg
        self.code = code
