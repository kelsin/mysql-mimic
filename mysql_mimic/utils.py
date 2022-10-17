import sys
from collections.abc import Iterator


class seq(Iterator):
    """Auto-incrementing sequence with an optional maximum size"""

    def __init__(self, size: int = None):
        self.size = size
        self.value = 0

    def __next__(self) -> int:
        value = self.value
        self.value = self.value + 1
        if self.size:
            self.value = self.value % self.size
        return value

    def reset(self) -> None:
        self.value = 0


def xor(a: bytes, b: bytes) -> bytes:
    # Fast XOR implementation, according to https://stackoverflow.com/questions/29408173/byte-operations-xor-in-python
    a, b = a[: len(b)], b[: len(a)]
    int_b = int.from_bytes(b, sys.byteorder)
    int_a = int.from_bytes(a, sys.byteorder)
    int_enc = int_b ^ int_a
    return int_enc.to_bytes(len(b), sys.byteorder)
