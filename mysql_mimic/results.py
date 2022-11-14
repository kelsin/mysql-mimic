from __future__ import annotations

import io
import struct
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Iterable, Sequence, Optional, Callable, Any, Union, Tuple, Dict

from mysql_mimic.errors import MysqlError
from mysql_mimic.types import ColumnType, str_len, uint_1, uint_2, uint_4
from mysql_mimic.charset import CharacterSet


Encoder = Callable[[Any, "ResultColumn"], bytes]


class ResultColumn:
    """
    Column data for a result set

    Args:
        name: column name
        type: column type
        character_set: column character set. Only relevant for string columns.
        text_encoder: Optionally override the function used to encode values for MySQL's text protocol
        binary_encoder: Optionally override the function used to encode values for MySQL's binary protocol
    """

    def __init__(
        self,
        name: str,
        type: ColumnType,  # pylint: disable=redefined-builtin
        character_set: CharacterSet = CharacterSet.utf8mb4,
        text_encoder: Optional[Encoder] = None,
        binary_encoder: Optional[Encoder] = None,
    ):
        self.name = name
        self.type = type
        self.character_set = character_set
        self.text_encoder = text_encoder or _TEXT_ENCODERS.get(type) or _unsupported
        self.binary_encoder = (
            binary_encoder or _BINARY_ENCODERS.get(type) or _unsupported
        )

    def text_encode(self, val: Any) -> bytes:
        return self.text_encoder(self, val)

    def binary_encode(self, val: Any) -> bytes:
        return self.binary_encoder(self, val)

    def __repr__(self) -> str:
        return f"ResultColumn({self.name} {self.type.name})"


@dataclass
class ResultSet:
    rows: Iterable[Sequence]
    columns: Sequence[ResultColumn]

    def __bool__(self) -> bool:
        return bool(self.columns)


AllowedColumn = Union[ResultColumn, str]
AllowedResult = Union[
    ResultSet, Tuple[Sequence[Sequence[Any]], Sequence[AllowedColumn]], None
]


def ensure_result_set(result: AllowedResult) -> ResultSet:
    if result is None:
        return ResultSet([], [])
    if isinstance(result, ResultSet):
        return result
    if isinstance(result, tuple):
        if len(result) != 2:
            raise MysqlError(
                f"Result tuple should be of size 2. Received: {len(result)}"
            )
        rows = result[0]
        columns = result[1]

        return ResultSet(
            rows=rows,
            columns=[_ensure_result_col(col, i, rows) for i, col in enumerate(columns)],
        )

    raise MysqlError(f"Unexpected result set type: {type(result)}")


def _ensure_result_col(
    column: AllowedColumn, idx: int, rows: Sequence[Sequence[Any]]
) -> ResultColumn:
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


def _find_first_non_null_value(
    idx: int, rows: Sequence[Sequence[Any]]
) -> Optional[Any]:
    for row in rows:
        value = row[idx]
        if value is not None:
            return value
    return None


def _binary_encode_tiny(col: ResultColumn, val: Any) -> bytes:
    return uint_1(int(bool(val)))


def _binary_encode_str(col: ResultColumn, val: Any) -> bytes:
    if not isinstance(val, bytes):
        val = str(val)

    if isinstance(val, str):
        val = val.encode(col.character_set.codec)

    return str_len(val)


def _binary_encode_date(col: ResultColumn, val: Any) -> bytes:
    if isinstance(val, (float, int)):
        val = datetime.fromtimestamp(val)

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


def _binary_encode_short(col: ResultColumn, val: Any) -> bytes:
    return struct.pack("<h", val)


def _binary_encode_int(col: ResultColumn, val: Any) -> bytes:
    return struct.pack("<i", val)


def _binary_encode_long(col: ResultColumn, val: Any) -> bytes:
    return struct.pack("<l", val)


def _binary_encode_longlong(col: ResultColumn, val: Any) -> bytes:
    return struct.pack("<q", val)


def _binary_encode_float(col: ResultColumn, val: Any) -> bytes:
    return struct.pack("<f", val)


def _binary_encode_double(col: ResultColumn, val: Any) -> bytes:
    return struct.pack("<d", val)


def _binary_encode_timedelta(col: ResultColumn, val: Any) -> bytes:
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


def _text_encode_str(col: ResultColumn, val: Any) -> bytes:
    if not isinstance(val, bytes):
        val = str(val)

    if isinstance(val, str):
        val = val.encode(col.character_set.codec)

    return val


def _text_encode_tiny(col: ResultColumn, val: Any) -> bytes:
    return _text_encode_str(col, int(val))


def _unsupported(col: ResultColumn, val: Any) -> bytes:
    raise MysqlError(f"Unsupported column type: {col.type}")


def _noop(col: ResultColumn, val: Any) -> bytes:
    return val


# Order matters
# bool is a subclass of int
# datetime is a subclass of date
_PY_TO_MYSQL_TYPE = {
    bool: ColumnType.TINY,
    datetime: ColumnType.DATETIME,
    str: ColumnType.STRING,
    bytes: ColumnType.BLOB,
    int: ColumnType.LONGLONG,
    float: ColumnType.DOUBLE,
    date: ColumnType.DATE,
    timedelta: ColumnType.TIME,
}


_TEXT_ENCODERS: Dict[ColumnType, Encoder] = {
    ColumnType.DECIMAL: _text_encode_str,
    ColumnType.TINY: _text_encode_tiny,
    ColumnType.SHORT: _text_encode_str,
    ColumnType.LONG: _text_encode_str,
    ColumnType.FLOAT: _text_encode_str,
    ColumnType.DOUBLE: _text_encode_str,
    ColumnType.NULL: _unsupported,
    ColumnType.TIMESTAMP: _text_encode_str,
    ColumnType.LONGLONG: _text_encode_str,
    ColumnType.INT24: _text_encode_str,
    ColumnType.DATE: _text_encode_str,
    ColumnType.TIME: _text_encode_str,
    ColumnType.DATETIME: _text_encode_str,
    ColumnType.YEAR: _text_encode_str,
    ColumnType.NEWDATE: _text_encode_str,
    ColumnType.VARCHAR: _text_encode_str,
    ColumnType.BIT: _text_encode_str,
    ColumnType.TIMESTAMP2: _unsupported,
    ColumnType.DATETIME2: _unsupported,
    ColumnType.TIME2: _unsupported,
    ColumnType.TYPED_ARRAY: _unsupported,
    ColumnType.INVALID: _unsupported,
    ColumnType.BOOL: _binary_encode_tiny,
    ColumnType.JSON: _text_encode_str,
    ColumnType.NEWDECIMAL: _text_encode_str,
    ColumnType.ENUM: _text_encode_str,
    ColumnType.SET: _text_encode_str,
    ColumnType.TINY_BLOB: _text_encode_str,
    ColumnType.MEDIUM_BLOB: _text_encode_str,
    ColumnType.LONG_BLOB: _text_encode_str,
    ColumnType.BLOB: _text_encode_str,
    ColumnType.VAR_STRING: _text_encode_str,
    ColumnType.STRING: _text_encode_str,
    ColumnType.GEOMETRY: _text_encode_str,
}

_BINARY_ENCODERS: Dict[ColumnType, Encoder] = {
    ColumnType.DECIMAL: _binary_encode_str,
    ColumnType.TINY: _binary_encode_tiny,
    ColumnType.SHORT: _binary_encode_short,
    ColumnType.LONG: _binary_encode_long,
    ColumnType.FLOAT: _binary_encode_float,
    ColumnType.DOUBLE: _binary_encode_double,
    ColumnType.NULL: _unsupported,
    ColumnType.TIMESTAMP: _binary_encode_date,
    ColumnType.LONGLONG: _binary_encode_longlong,
    ColumnType.INT24: _binary_encode_long,
    ColumnType.DATE: _binary_encode_date,
    ColumnType.TIME: _binary_encode_timedelta,
    ColumnType.DATETIME: _binary_encode_date,
    ColumnType.YEAR: _binary_encode_short,
    ColumnType.NEWDATE: _unsupported,
    ColumnType.VARCHAR: _binary_encode_str,
    ColumnType.BIT: _binary_encode_str,
    ColumnType.TIMESTAMP2: _unsupported,
    ColumnType.DATETIME2: _unsupported,
    ColumnType.TIME2: _unsupported,
    ColumnType.TYPED_ARRAY: _unsupported,
    ColumnType.INVALID: _unsupported,
    ColumnType.BOOL: _binary_encode_tiny,
    ColumnType.JSON: _binary_encode_str,
    ColumnType.NEWDECIMAL: _binary_encode_str,
    ColumnType.ENUM: _binary_encode_str,
    ColumnType.SET: _binary_encode_str,
    ColumnType.TINY_BLOB: _binary_encode_str,
    ColumnType.MEDIUM_BLOB: _binary_encode_str,
    ColumnType.LONG_BLOB: _binary_encode_str,
    ColumnType.BLOB: _binary_encode_str,
    ColumnType.VAR_STRING: _binary_encode_str,
    ColumnType.STRING: _binary_encode_str,
    ColumnType.GEOMETRY: _binary_encode_str,
}


def infer_type(val: Any) -> ColumnType:
    for py_type, my_type in _PY_TO_MYSQL_TYPE.items():
        if isinstance(val, py_type):
            return my_type
    return ColumnType.VARCHAR


class NullBitmap:
    """See https://dev.mysql.com/doc/internals/en/null-bitmap.html"""

    __slots__ = ("offset", "bitmap")

    def __init__(self, bitmap: bytearray, offset: int = 0):
        self.offset = offset
        self.bitmap = bitmap

    @classmethod
    def new(cls, num_bits: int, offset: int = 0) -> NullBitmap:
        bitmap = bytearray(cls._num_bytes(num_bits, offset))
        return cls(bitmap, offset)

    @classmethod
    def from_buffer(
        cls, buffer: io.BytesIO, num_bits: int, offset: int = 0
    ) -> NullBitmap:
        bitmap = bytearray(buffer.read(cls._num_bytes(num_bits, offset)))
        return cls(bitmap, offset)

    @classmethod
    def _num_bytes(cls, num_bits: int, offset: int) -> int:
        return (num_bits + 7 + offset) // 8

    def flip(self, i: int) -> None:
        byte_position, bit_position = self._pos(i)
        self.bitmap[byte_position] |= 1 << bit_position

    def is_flipped(self, i: int) -> bool:
        byte_position, bit_position = self._pos(i)
        return bool(self.bitmap[byte_position] & (1 << bit_position))

    def _pos(self, i: int) -> Tuple[int, int]:
        byte_position = (i + self.offset) // 8
        bit_position = (i + self.offset) % 8
        return byte_position, bit_position

    def __bytes__(self) -> bytes:
        return bytes(self.bitmap)

    def __repr__(self) -> str:
        return "".join(format(b, "08b") for b in self.bitmap)
