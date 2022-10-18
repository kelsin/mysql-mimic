import io
import struct

import pytest

from mysql_mimic import types


def test_column_definition() -> None:
    assert types.ColumnDefinition.NOT_NULL_FLAG == 1
    assert types.ColumnDefinition.PRI_KEY_FLAG == 2
    assert types.ColumnDefinition.FIELD_IS_INVISIBLE == 1 << 30
    assert (
        types.ColumnDefinition.NOT_NULL_FLAG | types.ColumnDefinition.PRI_KEY_FLAG == 3
    )


def test_capabilities() -> None:
    assert types.Capabilities.CLIENT_LONG_PASSWORD == 1
    assert types.Capabilities.CLIENT_FOUND_ROWS == 2
    assert types.Capabilities.CLIENT_PROTOCOL_41 == 512
    assert types.Capabilities.CLIENT_DEPRECATE_EOF == 1 << 24
    assert (
        types.Capabilities.CLIENT_LONG_PASSWORD | types.Capabilities.CLIENT_FOUND_ROWS
        == 3
    )


def test_server_status() -> None:
    assert types.ServerStatus.SERVER_STATUS_IN_TRANS == 1
    assert types.ServerStatus.SERVER_STATUS_AUTOCOMMIT == 2
    assert types.ServerStatus.SERVER_STATUS_LAST_ROW_SENT == 128
    assert (
        types.ServerStatus.SERVER_STATUS_IN_TRANS
        | types.ServerStatus.SERVER_STATUS_AUTOCOMMIT
        == 3
    )


def test_int_1() -> None:
    assert types.uint_1(0) == b"\x00"
    assert types.uint_1(1) == b"\x01"
    assert types.uint_1(255) == b"\xff"
    pytest.raises(struct.error, types.uint_1, 256)


def test_int_2() -> None:
    assert types.uint_2(0) == b"\x00\x00"
    assert types.uint_2(1) == b"\x01\x00"
    assert types.uint_2(255) == b"\xff\x00"
    assert types.uint_2(2**8) == b"\x00\x01"
    assert types.uint_2(2**16 - 1) == b"\xff\xff"
    pytest.raises(struct.error, types.uint_2, 2**16)


def test_int_3() -> None:
    assert types.uint_3(0) == b"\x00\x00\x00"
    assert types.uint_3(1) == b"\x01\x00\x00"
    assert types.uint_3(255) == b"\xff\x00\x00"
    assert types.uint_3(2**8) == b"\x00\x01\x00"
    assert types.uint_3(2**16) == b"\x00\x00\x01"
    pytest.raises(struct.error, types.uint_3, 2**24)


def test_int_4() -> None:
    assert types.uint_4(0) == b"\x00\x00\x00\x00"
    assert types.uint_4(1) == b"\x01\x00\x00\x00"
    assert types.uint_4(255) == b"\xff\x00\x00\x00"
    assert types.uint_4(2**8) == b"\x00\x01\x00\x00"
    assert types.uint_4(2**16) == b"\x00\x00\x01\x00"
    assert types.uint_4(2**24) == b"\x00\x00\x00\x01"
    pytest.raises(struct.error, types.uint_4, 2**32)


def test_int_6() -> None:
    assert types.uint_6(0) == b"\x00\x00\x00\x00\x00\x00"
    assert types.uint_6(1) == b"\x01\x00\x00\x00\x00\x00"
    assert types.uint_6(255) == b"\xff\x00\x00\x00\x00\x00"
    assert types.uint_6(2**8) == b"\x00\x01\x00\x00\x00\x00"
    assert types.uint_6(2**16) == b"\x00\x00\x01\x00\x00\x00"
    assert types.uint_6(2**24) == b"\x00\x00\x00\x01\x00\x00"
    assert types.uint_6(2**32) == b"\x00\x00\x00\x00\x01\x00"
    assert types.uint_6(2**40) == b"\x00\x00\x00\x00\x00\x01"
    pytest.raises(struct.error, types.uint_6, 2**48)


def test_int_8() -> None:
    assert types.uint_8(0) == b"\x00\x00\x00\x00\x00\x00\x00\x00"
    assert types.uint_8(1) == b"\x01\x00\x00\x00\x00\x00\x00\x00"
    assert types.uint_8(255) == b"\xff\x00\x00\x00\x00\x00\x00\x00"
    assert types.uint_8(2**8) == b"\x00\x01\x00\x00\x00\x00\x00\x00"
    assert types.uint_8(2**16) == b"\x00\x00\x01\x00\x00\x00\x00\x00"
    assert types.uint_8(2**24) == b"\x00\x00\x00\x01\x00\x00\x00\x00"
    assert types.uint_8(2**32) == b"\x00\x00\x00\x00\x01\x00\x00\x00"
    assert types.uint_8(2**56) == b"\x00\x00\x00\x00\x00\x00\x00\x01"
    pytest.raises(struct.error, types.uint_8, 2**64)


def test_int_len() -> None:
    assert types.uint_len(0) == b"\x00"
    assert types.uint_len(1) == b"\x01"
    assert types.uint_len(250) == b"\xfa"
    assert types.uint_len(251) == b"\xfc\xfb\x00"
    assert types.uint_len(2**8) == b"\xfc\x00\x01"
    assert types.uint_len(2**16) == b"\xfd\x00\x00\x01"
    assert types.uint_len(2**24) == b"\xfe\x00\x00\x00\x01\x00\x00\x00\x00"
    assert types.uint_len(2**32) == b"\xfe\x00\x00\x00\x00\x01\x00\x00\x00"
    assert types.uint_len(2**56) == b"\xfe\x00\x00\x00\x00\x00\x00\x00\x01"
    pytest.raises(struct.error, types.uint_len, 2**64)


def test_str_fixed() -> None:
    assert types.str_fixed(0, b"kelsin") == b""
    assert types.str_fixed(1, b"kelsin") == b"k"
    assert types.str_fixed(6, b"kelsin") == b"kelsin"
    assert types.str_fixed(8, b"kelsin") == b"kelsin\x00\x00"


def test_str_null() -> None:
    assert types.str_null(b"") == b"\x00"
    assert types.str_null(b"kelsin") == b"kelsin\x00"


def test_str_len() -> None:
    assert types.str_len(b"") == b"\x00"
    assert types.str_len(b"kelsin") == b"\x06kelsin"

    big_str = bytes(256)
    assert types.str_len(big_str) == b"\xfc\x00\x01" + big_str


def test_str_rest() -> None:
    assert types.str_rest(b"") == b""
    assert types.str_rest(b"kelsin") == b"kelsin"

    big_str = bytes(256)
    assert types.str_rest(big_str) == big_str


def test_read_int_1() -> None:
    reader = io.BytesIO(b"\x01")
    assert types.read_uint_1(reader) == 1
    reader = io.BytesIO(b"\x00\x00")
    assert types.read_uint_1(reader) == 0
    assert types.read_uint_1(reader) == 0


def test_read_int_2() -> None:
    reader = io.BytesIO(b"\x00\x01")
    assert types.read_uint_2(reader) == 256


def test_read_int_3() -> None:
    reader = io.BytesIO(b"\x00\x00\x01")
    assert types.read_uint_3(reader) == 2**16


def test_read_int_4() -> None:
    reader = io.BytesIO(b"\x01\x00\x00\x00")
    assert types.read_uint_4(reader) == 1
    reader = io.BytesIO(b"\x00\x00\x01\x00")
    assert types.read_uint_4(reader) == 2**16
    reader = io.BytesIO(b"\x00\x00\x00\x01")
    assert types.read_uint_4(reader) == 2**24


def test_read_int_6() -> None:
    reader = io.BytesIO(b"\x01\x00\x00\x00\x00\x00")
    assert types.read_uint_6(reader) == 1
    reader = io.BytesIO(b"\x00\x00\x00\x00\x01\x00")
    assert types.read_uint_6(reader) == 2**32
    reader = io.BytesIO(b"\x00\x00\x00\x00\x00\x01")
    assert types.read_uint_6(reader) == 2**40


def test_read_int_8() -> None:
    reader = io.BytesIO(b"\x01\x00\x00\x00\x00\x00\x00\x00")
    assert types.read_uint_8(reader) == 1
    reader = io.BytesIO(b"\x00\x00\x00\x00\x00\x00\x01\x00")
    assert types.read_uint_8(reader) == 2**48
    reader = io.BytesIO(b"\x00\x00\x00\x00\x00\x00\x00\x01")
    assert types.read_uint_8(reader) == 2**56


def test_read_int_len() -> None:
    reader = io.BytesIO(b"\x01")
    assert types.read_uint_len(reader) == 1
    reader = io.BytesIO(b"\xfa")
    assert types.read_uint_len(reader) == 250
    reader = io.BytesIO(b"\xfc\x00\x01")
    assert types.read_uint_len(reader) == 256
    reader = io.BytesIO(b"\xfd\x00\x00\x01")
    assert types.read_uint_len(reader) == 2**16
    reader = io.BytesIO(b"\xfe\x00\x00\x00\x00\x00\x00\x00\x01")
    assert types.read_uint_len(reader) == 2**56


def test_read_str_fixed() -> None:
    reader = io.BytesIO(b"kelsin")
    assert types.read_str_fixed(reader, 1) == b"k"
    assert types.read_str_fixed(reader, 2) == b"el"
    assert types.read_str_fixed(reader, 3) == b"sin"


def test_read_str_null() -> None:
    reader = io.BytesIO(b"\x00")
    assert types.read_str_null(reader) == b""
    reader = io.BytesIO(b"kelsin\x00")
    assert types.read_str_null(reader) == b"kelsin"
    reader = io.BytesIO(b"kelsin\x00foo")
    assert types.read_str_null(reader) == b"kelsin"


def test_read_str_len() -> None:
    reader = io.BytesIO(b"\x01kelsin")
    assert types.read_str_len(reader) == b"k"

    big_str = bytes(256)
    reader = io.BytesIO(b"\xfc\x00\x01" + big_str)
    assert types.read_str_len(reader) == big_str


def test_read_str_rest() -> None:
    reader = io.BytesIO(b"kelsin")
    assert types.read_str_rest(reader) == b"kelsin"
    reader = io.BytesIO(b"kelsin\x00foo")
    assert types.read_str_rest(reader) == b"kelsin\x00foo"
