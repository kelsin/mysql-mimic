import sys
from collections.abc import Iterator


class seq(Iterator):
    """Auto-incrementing sequence with an optional maximum size"""

    def __init__(self, size=None):
        self.size = size
        self.value = 0

    def __next__(self):
        value = self.value
        self.value = self.value + 1
        if self.size:
            self.value = self.value % self.size
        return value

    def reset(self):
        self.value = 0


def xor(a, b, byteorder=sys.byteorder):
    # Fast XOR implementation, according to https://stackoverflow.com/questions/29408173/byte-operations-xor-in-python
    a, b = a[: len(b)], b[: len(a)]
    int_b = int.from_bytes(b, byteorder)
    int_a = int.from_bytes(a, byteorder)
    int_enc = int_b ^ int_a
    return int_enc.to_bytes(len(b), byteorder)


def ensure_list(value):
    if value is None:
        return []
    return value if isinstance(value, (list, tuple, set)) else [value]
