import pytest

from mysql_mimic.errors import MysqlError
from mysql_mimic.stream import MysqlStream


class MockReader:
    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    async def read(self, n: int) -> bytes:
        start = self.pos
        end = self.pos + n
        self.pos = end

        return self.data[start:end]

    readexactly = read


class MockWriter:
    def __init__(self) -> None:
        self.data = b""

    def write(self, data: bytes) -> None:
        self.data += data

    async def drain(self) -> None:
        return


def test_seq() -> None:
    s1 = MysqlStream(reader=None, writer=None)  # type: ignore
    assert next(s1.seq) == 0
    assert next(s1.seq) == 1
    assert next(s1.seq) == 2
    s2 = MysqlStream(reader=None, writer=None)  # type: ignore
    assert next(s2.seq) == 0
    assert next(s2.seq) == 1
    assert next(s2.seq) == 2


def test_reset_seq() -> None:
    s = MysqlStream(reader=None, writer=None)  # type: ignore
    assert next(s.seq) == 0
    assert next(s.seq) == 1
    s.reset_seq()
    assert next(s.seq) == 0
    assert next(s.seq) == 1


@pytest.mark.asyncio
async def test_bad_seq_read() -> None:
    reader = MockReader(b"\x00\x00\x00\x01")
    s = MysqlStream(reader=reader, writer=None)  # type: ignore
    with pytest.raises(MysqlError):
        await s.read()


@pytest.mark.asyncio
async def test_empty_read() -> None:
    reader = MockReader(b"\x00\x00\x00\x00")
    s = MysqlStream(reader=reader, writer=None)  # type: ignore
    assert await s.read() == b""
    assert next(s.seq) == 1


@pytest.mark.asyncio
async def test_small_read() -> None:
    reader = MockReader(b"\x01\x00\x00\x00k")
    s = MysqlStream(reader=reader, writer=None)  # type: ignore
    assert await s.read() == b"k"
    assert next(s.seq) == 1


@pytest.mark.asyncio
async def test_medium_read() -> None:
    reader = MockReader(b"\xff\xff\x00\x00" + bytes(0xFFFF))
    s = MysqlStream(reader=reader, writer=None)  # type: ignore
    assert await s.read() == bytes(0xFFFF)
    assert next(s.seq) == 1


@pytest.mark.asyncio
async def test_edge_read() -> None:
    reader = MockReader(b"\xff\xff\xff\x00" + bytes(0xFFFFFF) + b"\x00\x00\x00\x01")
    s = MysqlStream(reader=reader, writer=None)  # type: ignore
    assert await s.read() == bytes(0xFFFFFF)
    assert next(s.seq) == 2


@pytest.mark.asyncio
async def test_large_read() -> None:
    reader = MockReader(
        b"\xff\xff\xff\x00" + bytes(0xFFFFFF) + b"\x06\x00\x00\x01" + b"kelsin"
    )
    s = MysqlStream(reader=reader, writer=None)  # type: ignore
    assert await s.read() == bytes(0xFFFFFF) + b"kelsin"
    assert next(s.seq) == 2


@pytest.mark.asyncio
async def test_empty_write() -> None:
    writer = MockWriter()
    s = MysqlStream(reader=None, writer=writer)  # type: ignore
    await s.write(b"")
    assert writer.data == b"\x00\x00\x00\x00"
    assert next(s.seq) == 1


@pytest.mark.asyncio
async def test_small_write() -> None:
    writer = MockWriter()
    s = MysqlStream(reader=None, writer=writer)  # type: ignore
    await s.write(b"kelsin")
    assert writer.data == b"\x06\x00\x00\x00kelsin"
    assert next(s.seq) == 1


@pytest.mark.asyncio
async def test_medium_write() -> None:
    writer = MockWriter()
    s = MysqlStream(reader=None, writer=writer)  # type: ignore
    await s.write(bytes(0xFFFF))
    assert writer.data == b"\xff\xff\x00\x00" + bytes(0xFFFF)
    assert next(s.seq) == 1


@pytest.mark.asyncio
async def test_edge_write() -> None:
    writer = MockWriter()
    s = MysqlStream(reader=None, writer=writer)  # type: ignore
    await s.write(bytes(0xFFFFFF))
    assert writer.data == b"\xff\xff\xff\x00" + bytes(0xFFFFFF) + b"\x00\x00\x00\x01"
    assert next(s.seq) == 2


@pytest.mark.asyncio
async def test_large_write() -> None:
    writer = MockWriter()
    s = MysqlStream(reader=None, writer=writer)  # type: ignore
    await s.write(bytes(0xFFFFFF) + b"kelsin")
    assert (
        writer.data == b"\xff\xff\xff\x00" + bytes(0xFFFFFF) + b"\x06\x00\x00\x01kelsin"
    )
    assert next(s.seq) == 2
