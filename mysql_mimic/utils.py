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
