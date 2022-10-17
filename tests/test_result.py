from typing import Any

import pytest

from mysql_mimic.errors import MysqlError
from mysql_mimic.results import ensure_result_set


@pytest.mark.parametrize(
    "result",
    [
        [1, 2],
        ([[1, 2]], ["a", "b"], ["a", "b"]),
        ([[1, 2]], [1, 2]),
    ],
)
def test_ensure_result_set__invalid(result: Any) -> None:
    with pytest.raises(MysqlError):
        ensure_result_set(result)
