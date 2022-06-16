import itertools
import struct

from mysql_mimic.types import int_3, int_1


class ConnectionClosed(Exception):
    pass


class MysqlStream:
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self._seq = None
        self.reset_seq()

    def seq(self):
        return next(self._seq)

    async def read(self):
        data = b""
        while True:
            seq_num = await self.reader.read(4)

            if not seq_num:
                raise ConnectionClosed()

            i = struct.unpack("<I", seq_num)[0]
            l = i & 0x00FFFFFF
            s = (i & 0xFF000000) >> 24

            expected = self.seq()
            if s != expected:
                raise ValueError(f"Expected seq({expected}) got seq({s})")

            if l == 0:
                return data

            data += await self.reader.read(l)

            if l < 0xFFFFFF:
                return data

    def write(self, data):
        while True:
            # Grab first 0xFFFFFF bytes to send
            payload = data[:0xFFFFFF]
            data = data[0xFFFFFF:]

            payload_length = int_3(len(payload))
            sequence_id = int_1(self.seq())

            self.writer.write(payload_length + sequence_id + payload)

            # We are done unless len(send) == 0xFFFFFF
            if len(payload) != 0xFFFFFF:
                return

    def reset_seq(self):
        # Sequence number is one byte and wraps around
        self._seq = itertools.cycle(range(0xFF + 1))
