from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Callable, Any, Iterable, Sequence

from mysql_mimic.types import ColumnType, CharacterSet


@dataclass
class ResultColumn:
    name: str
    type: ColumnType
    character_set: CharacterSet
    encoder: Callable[[Any], bytes]


@dataclass
class ResultSet:
    rows: Iterable[Sequence]
    columns: Sequence[ResultColumn]


def ensure_result_set(result):
    if isinstance(result, ResultSet):
        return result
    if isinstance(result, tuple):
        if len(result) != 2:
            raise ValueError(
                f"Result tuple should be of size 2. Received: {len(result)}"
            )
        rows = result[0]
        columns = result[1]

        columns = [_ensure_result_col(col, i, rows) for i, col in enumerate(columns)]
        return ResultSet(
            rows=rows,
            columns=columns,
        )

    raise ValueError(f"Unexpected result set type: {type(result)}")


def _ensure_result_col(column, idx, rows):
    if isinstance(column, ResultColumn):
        return column

    if isinstance(column, str):
        value = _find_first_non_null_value(idx, rows)
        type_, charset, encoder = infer_encoder(value)
        return ResultColumn(
            name=column,
            type=type_,
            character_set=charset,
            encoder=encoder,
        )

    raise ValueError(f"Unexpected result column value: {column}")


def _find_first_non_null_value(idx, rows):
    for row in rows:
        value = row[idx]
        if value is not None:
            return value
    return None


def _encode_bool(val):
    return _encode_str(int(val))


def _encode_bytes(val):
    return val


def _encode_str(val):
    return str(val).encode("utf-8")


def _encode_number(val):
    return str(val).encode("ascii")


_DEFAULT_ENCODERS = {
    # Order matters
    # `bool` should be checked before `int`
    # `datetime` should be checked before `date`
    bool: (ColumnType.TINY, CharacterSet.ASCII, _encode_bool),
    datetime: (ColumnType.DATETIME, CharacterSet.UTF8, _encode_str),
    str: (ColumnType.VARCHAR, CharacterSet.UTF8, _encode_str),
    bytes: (ColumnType.BLOB, CharacterSet.ASCII, _encode_bytes),
    int: (ColumnType.LONGLONG, CharacterSet.ASCII, _encode_number),
    float: (ColumnType.DOUBLE, CharacterSet.ASCII, _encode_number),
    date: (ColumnType.DATE, CharacterSet.UTF8, _encode_str),
    timedelta: (ColumnType.TIME, CharacterSet.UTF8, _encode_str),
}
_FALLBACK_ENCODER = (ColumnType.VARCHAR, CharacterSet.UTF8, _encode_str)


def infer_encoder(val):
    for py_type, result in _DEFAULT_ENCODERS.items():
        if isinstance(val, py_type):
            return result
    return _FALLBACK_ENCODER
