import struct
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Iterable, Sequence, Optional

from mysql_mimic.errors import MysqlError
from mysql_mimic.types import ColumnType, str_len, uint_1, uint_2, uint_4
from mysql_mimic.charset import CharacterSet


@dataclass
class ResultColumn:
    """Column data for a result set"""

    name: str
    type: ColumnType
    character_set: CharacterSet = CharacterSet.utf8mb4


@dataclass
class ResultSet:
    rows: Iterable[Sequence]
    columns: Sequence[ResultColumn]

    def __bool__(self):
        return bool(self.columns)


@dataclass
class Column:
    """
    Column data for a SHOW COLUMNS statement

    https://dev.mysql.com/doc/refman/8.0/en/show-columns.html
    """

    name: str
    type: str
    collation: str = "NULL"
    null: str = "YES"
    key: Optional[str] = None
    default: Optional[str] = None
    extra: Optional[str] = None
    privileges: Optional[str] = None
    comment: Optional[str] = None


def ensure_result_set(result):
    if isinstance(result, ResultSet):
        return result
    if isinstance(result, tuple):
        if len(result) != 2:
            raise MysqlError(
                f"Result tuple should be of size 2. Received: {len(result)}"
            )
        rows = result[0]
        columns = result[1]

        columns = [_ensure_result_col(col, i, rows) for i, col in enumerate(columns)]
        return ResultSet(
            rows=rows,
            columns=columns,
        )

    raise MysqlError(f"Unexpected result set type: {type(result)}")


def _ensure_result_col(column, idx, rows):
    if isinstance(column, ResultColumn):
        return column

    if isinstance(column, str):
        value = _find_first_non_null_value(idx, rows)
        type_ = infer_type(value)
        return ResultColumn(
            name=column,
            type=type_,
        )

    raise MysqlError(f"Unexpected result column value: {column}")


def _find_first_non_null_value(idx, rows):
    for row in rows:
        value = row[idx]
        if value is not None:
            return value
    return None


def _binary_encode_bool(val):
    return uint_1(int(val))


def _binary_encode_str(val):
    return str_len(str(val).encode("utf-8"))


def _binary_encode_bytes(val):
    return str_len(val)


def _binary_encode_date(val):
    year = val.year
    month = val.month
    day = val.day

    if isinstance(val, datetime):
        hour = val.hour
        minute = val.minute
        second = val.second
        microsecond = val.microsecond
    else:
        hour = minute = second = microsecond = 0

    if microsecond == 0:
        if hour == minute == second == 0:
            if year == month == day == 0:
                return uint_1(0)
            return b"".join([uint_1(4), uint_2(year), uint_1(month), uint_1(day)])
        return b"".join(
            [
                uint_1(7),
                uint_2(year),
                uint_1(month),
                uint_1(day),
                uint_1(hour),
                uint_1(minute),
                uint_1(second),
            ]
        )
    return b"".join(
        [
            uint_1(11),
            uint_2(year),
            uint_1(month),
            uint_1(day),
            uint_1(hour),
            uint_1(minute),
            uint_1(second),
            uint_4(microsecond),
        ]
    )


def _binary_encode_int(val):
    return struct.pack("<q", val)


def _binary_encode_float(val):
    return struct.pack("<d", val)


def _binary_encode_timedelta(val):
    days = abs(val.days)
    hours, remainder = divmod(abs(val.seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    microseconds = val.microseconds
    is_negative = val.total_seconds() < 0

    if microseconds == 0:
        if days == hours == minutes == seconds == 0:
            return uint_1(0)
        return b"".join(
            [
                uint_1(8),
                uint_1(is_negative),
                uint_4(days),
                uint_1(hours),
                uint_1(minutes),
                uint_1(seconds),
            ]
        )
    return b"".join(
        [
            uint_1(12),
            uint_1(is_negative),
            uint_4(days),
            uint_1(hours),
            uint_1(minutes),
            uint_1(seconds),
            uint_4(microseconds),
        ]
    )


# Order matters
# bool is a subclass of int
# datetime is a subclass of date
_ENCODERS = {
    # python type: (mysql type, text encoder, binary encoder),
    bool: (ColumnType.TINY, lambda v: str(int(v)), _binary_encode_bool),
    datetime: (ColumnType.DATETIME, str, _binary_encode_date),
    str: (ColumnType.STRING, lambda v: v, _binary_encode_str),
    bytes: (ColumnType.BLOB, lambda v: v, _binary_encode_bytes),
    int: (ColumnType.LONGLONG, str, _binary_encode_int),
    float: (ColumnType.DOUBLE, str, _binary_encode_float),
    date: (ColumnType.DATE, str, _binary_encode_date),
    timedelta: (ColumnType.TIME, str, _binary_encode_timedelta),
}


def binary_encode(val):
    for py_type, (_, _, encoder) in _ENCODERS.items():
        if isinstance(val, py_type):
            return encoder(val)
    return _binary_encode_str(val)


def text_encode(val):
    for py_type, (_, encoder, _) in _ENCODERS.items():
        if isinstance(val, py_type):
            return encoder(val)
    return str(val)


def infer_type(val):
    for py_type, (my_type, _, _) in _ENCODERS.items():
        if isinstance(val, py_type):
            return my_type
    return ColumnType.VARCHAR


class NullBitmap:
    """See https://dev.mysql.com/doc/internals/en/null-bitmap.html"""

    __slots__ = ("offset", "bitmap")

    def __init__(self, bitmap, offset=0):
        self.offset = offset
        self.bitmap = bitmap

    @classmethod
    def new(cls, num_bits, offset=0):
        bitmap = bytearray(cls._num_bytes(num_bits, offset))
        return cls(bitmap, offset)

    @classmethod
    def from_buffer(cls, buffer, num_bits, offset=0):
        bitmap = bytearray(buffer.read(cls._num_bytes(num_bits, offset)))
        return cls(bitmap, offset)

    @classmethod
    def _num_bytes(cls, num_bits, offset):
        return (num_bits + 7 + offset) // 8

    def flip(self, i):
        byte_position, bit_position = self._pos(i)
        self.bitmap[byte_position] |= 1 << bit_position

    def is_flipped(self, i):
        byte_position, bit_position = self._pos(i)
        return bool(self.bitmap[byte_position] & (1 << bit_position))

    def _pos(self, i):
        byte_position = (i + self.offset) // 8
        bit_position = (i + self.offset) % 8
        return byte_position, bit_position

    def __bytes__(self):
        return bytes(self.bitmap)
