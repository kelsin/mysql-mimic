import io
import struct
import unittest

from mysql_mimic import types


class TestTypes(unittest.TestCase):
    def test_column_definition(self):
        self.assertEqual(types.ColumnDefinition.NOT_NULL_FLAG, 1)
        self.assertEqual(types.ColumnDefinition.PRI_KEY_FLAG, 2)
        self.assertEqual(types.ColumnDefinition.FIELD_IS_INVISIBLE, 1 << 30)
        self.assertEqual(
            types.ColumnDefinition.NOT_NULL_FLAG | types.ColumnDefinition.PRI_KEY_FLAG,
            3,
        )

    def test_capabilities(self):
        self.assertEqual(types.Capabilities.CLIENT_LONG_PASSWORD, 1)
        self.assertEqual(types.Capabilities.CLIENT_FOUND_ROWS, 2)
        self.assertEqual(types.Capabilities.CLIENT_PROTOCOL_41, 512)
        self.assertEqual(types.Capabilities.CLIENT_DEPRECATE_EOF, 1 << 24)
        self.assertEqual(
            types.Capabilities.CLIENT_LONG_PASSWORD
            | types.Capabilities.CLIENT_FOUND_ROWS,
            3,
        )

    def test_server_status(self):
        self.assertEqual(types.ServerStatus.SERVER_STATUS_IN_TRANS, 1)
        self.assertEqual(types.ServerStatus.SERVER_STATUS_AUTOCOMMIT, 2)
        self.assertEqual(types.ServerStatus.SERVER_STATUS_LAST_ROW_SENT, 128)
        self.assertEqual(
            types.ServerStatus.SERVER_STATUS_IN_TRANS
            | types.ServerStatus.SERVER_STATUS_AUTOCOMMIT,
            3,
        )

    def test_int_1(self):
        self.assertEqual(types.int_1(0), b"\x00")
        self.assertEqual(types.int_1(1), b"\x01")
        self.assertEqual(types.int_1(255), b"\xff")
        self.assertRaises(struct.error, types.int_1, 256)

    def test_int_2(self):
        self.assertEqual(types.int_2(0), b"\x00\x00")
        self.assertEqual(types.int_2(1), b"\x01\x00")
        self.assertEqual(types.int_2(255), b"\xff\x00")
        self.assertEqual(types.int_2(2**8), b"\x00\x01")
        self.assertEqual(types.int_2(2**16 - 1), b"\xff\xff")
        self.assertRaises(struct.error, types.int_2, 2**16)

    def test_int_3(self):
        self.assertEqual(types.int_3(0), b"\x00\x00\x00")
        self.assertEqual(types.int_3(1), b"\x01\x00\x00")
        self.assertEqual(types.int_3(255), b"\xff\x00\x00")
        self.assertEqual(types.int_3(2**8), b"\x00\x01\x00")
        self.assertEqual(types.int_3(2**16), b"\x00\x00\x01")
        self.assertRaises(struct.error, types.int_3, 2**24)

    def test_int_4(self):
        self.assertEqual(types.int_4(0), b"\x00\x00\x00\x00")
        self.assertEqual(types.int_4(1), b"\x01\x00\x00\x00")
        self.assertEqual(types.int_4(255), b"\xff\x00\x00\x00")
        self.assertEqual(types.int_4(2**8), b"\x00\x01\x00\x00")
        self.assertEqual(types.int_4(2**16), b"\x00\x00\x01\x00")
        self.assertEqual(types.int_4(2**24), b"\x00\x00\x00\x01")
        self.assertRaises(struct.error, types.int_4, 2**32)

    def test_int_6(self):
        self.assertEqual(types.int_6(0), b"\x00\x00\x00\x00\x00\x00")
        self.assertEqual(types.int_6(1), b"\x01\x00\x00\x00\x00\x00")
        self.assertEqual(types.int_6(255), b"\xff\x00\x00\x00\x00\x00")
        self.assertEqual(types.int_6(2**8), b"\x00\x01\x00\x00\x00\x00")
        self.assertEqual(types.int_6(2**16), b"\x00\x00\x01\x00\x00\x00")
        self.assertEqual(types.int_6(2**24), b"\x00\x00\x00\x01\x00\x00")
        self.assertEqual(types.int_6(2**32), b"\x00\x00\x00\x00\x01\x00")
        self.assertEqual(types.int_6(2**40), b"\x00\x00\x00\x00\x00\x01")
        self.assertRaises(struct.error, types.int_6, 2**48)

    def test_int_8(self):
        self.assertEqual(types.int_8(0), b"\x00\x00\x00\x00\x00\x00\x00\x00")
        self.assertEqual(types.int_8(1), b"\x01\x00\x00\x00\x00\x00\x00\x00")
        self.assertEqual(types.int_8(255), b"\xff\x00\x00\x00\x00\x00\x00\x00")
        self.assertEqual(types.int_8(2**8), b"\x00\x01\x00\x00\x00\x00\x00\x00")
        self.assertEqual(types.int_8(2**16), b"\x00\x00\x01\x00\x00\x00\x00\x00")
        self.assertEqual(types.int_8(2**24), b"\x00\x00\x00\x01\x00\x00\x00\x00")
        self.assertEqual(types.int_8(2**32), b"\x00\x00\x00\x00\x01\x00\x00\x00")
        self.assertEqual(types.int_8(2**56), b"\x00\x00\x00\x00\x00\x00\x00\x01")
        self.assertRaises(struct.error, types.int_8, 2**64)

    def test_int_len(self):
        self.assertEqual(types.int_len(0), b"\x00")
        self.assertEqual(types.int_len(1), b"\x01")
        self.assertEqual(types.int_len(250), b"\xfa")
        self.assertEqual(types.int_len(251), b"\xfc\xfb\x00")
        self.assertEqual(types.int_len(2**8), b"\xfc\x00\x01")
        self.assertEqual(types.int_len(2**16), b"\xfd\x00\x00\x01")
        self.assertEqual(
            types.int_len(2**24), b"\xfe\x00\x00\x00\x01\x00\x00\x00\x00"
        )
        self.assertEqual(
            types.int_len(2**32), b"\xfe\x00\x00\x00\x00\x01\x00\x00\x00"
        )
        self.assertEqual(
            types.int_len(2**56), b"\xfe\x00\x00\x00\x00\x00\x00\x00\x01"
        )
        self.assertRaises(struct.error, types.int_len, 2**64)

    def test_str_fixed(self):
        self.assertEqual(types.str_fixed(0, b"kelsin"), b"")
        self.assertEqual(types.str_fixed(1, b"kelsin"), b"k")
        self.assertEqual(types.str_fixed(6, b"kelsin"), b"kelsin")
        self.assertEqual(types.str_fixed(8, b"kelsin"), b"kelsin\x00\x00")

    def test_str_null(self):
        self.assertEqual(types.str_null(b""), b"\x00")
        self.assertEqual(types.str_null(b"kelsin"), b"kelsin\x00")

    def test_str_len(self):
        self.assertEqual(types.str_len(b""), b"\x00")
        self.assertEqual(types.str_len(b"kelsin"), b"\x06kelsin")

        big_str = bytes(256)
        self.assertEqual(types.str_len(big_str), b"\xfc\x00\x01" + big_str)

    def test_str_rest(self):
        self.assertEqual(types.str_rest(b""), b"")
        self.assertEqual(types.str_rest(b"kelsin"), b"kelsin")

        big_str = bytes(256)
        self.assertEqual(types.str_rest(big_str), big_str)

    def test_read_int_1(self):
        reader = io.BytesIO(b"\x01")
        self.assertEqual(types.read_int_1(reader), 1)
        reader = io.BytesIO(b"\x00\x00")
        self.assertEqual(types.read_int_1(reader), 0)
        self.assertEqual(types.read_int_1(reader), 0)

    def test_read_int_2(self):
        reader = io.BytesIO(b"\x00\x01")
        self.assertEqual(types.read_int_2(reader), 256)

    def test_read_int_3(self):
        reader = io.BytesIO(b"\x00\x00\x01")
        self.assertEqual(types.read_int_3(reader), 2**16)

    def test_read_int_4(self):
        reader = io.BytesIO(b"\x01\x00\x00\x00")
        self.assertEqual(types.read_int_4(reader), 1)
        reader = io.BytesIO(b"\x00\x00\x01\x00")
        self.assertEqual(types.read_int_4(reader), 2**16)
        reader = io.BytesIO(b"\x00\x00\x00\x01")
        self.assertEqual(types.read_int_4(reader), 2**24)

    def test_read_int_6(self):
        reader = io.BytesIO(b"\x01\x00\x00\x00\x00\x00")
        self.assertEqual(types.read_int_6(reader), 1)
        reader = io.BytesIO(b"\x00\x00\x00\x00\x01\x00")
        self.assertEqual(types.read_int_6(reader), 2**32)
        reader = io.BytesIO(b"\x00\x00\x00\x00\x00\x01")
        self.assertEqual(types.read_int_6(reader), 2**40)

    def test_read_int_8(self):
        reader = io.BytesIO(b"\x01\x00\x00\x00\x00\x00\x00\x00")
        self.assertEqual(types.read_int_8(reader), 1)
        reader = io.BytesIO(b"\x00\x00\x00\x00\x00\x00\x01\x00")
        self.assertEqual(types.read_int_8(reader), 2**48)
        reader = io.BytesIO(b"\x00\x00\x00\x00\x00\x00\x00\x01")
        self.assertEqual(types.read_int_8(reader), 2**56)

    def test_read_int_len(self):
        reader = io.BytesIO(b"\x01")
        self.assertEqual(types.read_int_len(reader), 1)
        reader = io.BytesIO(b"\xfa")
        self.assertEqual(types.read_int_len(reader), 250)
        reader = io.BytesIO(b"\xfc\x00\x01")
        self.assertEqual(types.read_int_len(reader), 256)
        reader = io.BytesIO(b"\xfd\x00\x00\x01")
        self.assertEqual(types.read_int_len(reader), 2**16)
        reader = io.BytesIO(b"\xfe\x00\x00\x00\x00\x00\x00\x00\x01")
        self.assertEqual(types.read_int_len(reader), 2**56)

    def test_read_str_fixed(self):
        reader = io.BytesIO(b"kelsin")
        self.assertEqual(types.read_str_fixed(reader, 1), b"k")
        self.assertEqual(types.read_str_fixed(reader, 2), b"el")
        self.assertEqual(types.read_str_fixed(reader, 3), b"sin")

    def test_read_str_null(self):
        reader = io.BytesIO(b"\x00")
        self.assertEqual(types.read_str_null(reader), b"")
        reader = io.BytesIO(b"kelsin\x00")
        self.assertEqual(types.read_str_null(reader), b"kelsin")
        reader = io.BytesIO(b"kelsin\x00foo")
        self.assertEqual(types.read_str_null(reader), b"kelsin")

    def test_read_str_len(self):
        reader = io.BytesIO(b"\x01kelsin")
        self.assertEqual(types.read_str_len(reader), b"k")

        big_str = bytes(256)
        reader = io.BytesIO(b"\xfc\x00\x01" + big_str)
        self.assertEqual(types.read_str_len(reader), big_str)

    def test_read_str_rest(self):
        reader = io.BytesIO(b"kelsin")
        self.assertEqual(types.read_str_rest(reader), b"kelsin")
        reader = io.BytesIO(b"kelsin\x00foo")
        self.assertEqual(types.read_str_rest(reader), b"kelsin\x00foo")
