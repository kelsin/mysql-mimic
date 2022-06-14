import unittest

from mysql_mimic.stream import MysqlStream


class MockReader:
    def __init__(self, data):
        self.data = data
        self.pos = 0

    async def read(self, size):
        start = self.pos
        end = self.pos + size
        self.pos = end

        return self.data[start:end]


class MockWriter:
    def __init__(self):
        self.data = b""

    def write(self, data):
        self.data += data


class TestMysqlStream(unittest.IsolatedAsyncioTestCase):
    def test_seq(self):
        s1 = MysqlStream(reader=None, writer=None)
        self.assertEqual(s1.seq(), 0)
        self.assertEqual(s1.seq(), 1)
        self.assertEqual(s1.seq(), 2)
        s2 = MysqlStream(reader=None, writer=None)
        self.assertEqual(s2.seq(), 0)
        self.assertEqual(s2.seq(), 1)
        self.assertEqual(s2.seq(), 2)

    def test_reset_seq(self):
        s = MysqlStream(reader=None, writer=None)
        self.assertEqual(s.seq(), 0)
        self.assertEqual(s.seq(), 1)
        s.reset_seq()
        self.assertEqual(s.seq(), 0)
        self.assertEqual(s.seq(), 1)

    async def test_bad_seq_read(self):
        reader = MockReader(b"\x00\x00\x00\x01")
        s = MysqlStream(reader=reader, writer=None)
        with self.assertRaises(ValueError):
            await s.read()

    async def test_empty_read(self):
        reader = MockReader(b"\x00\x00\x00\x00")
        s = MysqlStream(reader=reader, writer=None)
        self.assertEqual(await s.read(), b"")
        self.assertEqual(s.seq(), 1)

    async def test_small_read(self):
        reader = MockReader(b"\x01\x00\x00\x00k")
        s = MysqlStream(reader=reader, writer=None)
        self.assertEqual(await s.read(), b"k")
        self.assertEqual(s.seq(), 1)

    async def test_medium_read(self):
        reader = MockReader(b"\xff\xff\x00\x00" + bytes(0xFFFF))
        s = MysqlStream(reader=reader, writer=None)
        self.assertEqual(await s.read(), bytes(0xFFFF))
        self.assertEqual(s.seq(), 1)

    async def test_edge_read(self):
        reader = MockReader(b"\xff\xff\xff\x00" + bytes(0xFFFFFF) + b"\x00\x00\x00\x01")
        s = MysqlStream(reader=reader, writer=None)
        self.assertEqual(await s.read(), bytes(0xFFFFFF))
        self.assertEqual(s.seq(), 2)

    async def test_large_read(self):
        reader = MockReader(
            b"\xff\xff\xff\x00" + bytes(0xFFFFFF) + b"\x06\x00\x00\x01" + b"kelsin"
        )
        s = MysqlStream(reader=reader, writer=None)
        self.assertEqual(await s.read(), bytes(0xFFFFFF) + b"kelsin")
        self.assertEqual(s.seq(), 2)

    async def test_empty_write(self):
        writer = MockWriter()
        s = MysqlStream(reader=None, writer=writer)
        s.write(b"")
        self.assertEqual(writer.data, b"\x00\x00\x00\x00")
        self.assertEqual(s.seq(), 1)

    async def test_small_write(self):
        writer = MockWriter()
        s = MysqlStream(reader=None, writer=writer)
        s.write(b"kelsin")
        self.assertEqual(writer.data, b"\x06\x00\x00\x00kelsin")
        self.assertEqual(s.seq(), 1)

    async def test_medium_write(self):
        writer = MockWriter()
        s = MysqlStream(reader=None, writer=writer)
        s.write(bytes(0xFFFF))
        self.assertEqual(writer.data, b"\xff\xff\x00\x00" + bytes(0xFFFF))
        self.assertEqual(s.seq(), 1)

    async def test_edge_write(self):
        writer = MockWriter()
        s = MysqlStream(reader=None, writer=writer)
        s.write(bytes(0xFFFFFF))
        self.assertEqual(
            writer.data, b"\xff\xff\xff\x00" + bytes(0xFFFFFF) + b"\x00\x00\x00\x01"
        )
        self.assertEqual(s.seq(), 2)

    async def test_large_write(self):
        writer = MockWriter()
        s = MysqlStream(reader=None, writer=writer)
        s.write(bytes(0xFFFFFF) + b"kelsin")
        self.assertEqual(
            writer.data,
            b"\xff\xff\xff\x00" + bytes(0xFFFFFF) + b"\x06\x00\x00\x01kelsin",
        )
        self.assertEqual(s.seq(), 2)
