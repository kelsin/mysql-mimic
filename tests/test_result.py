from typing import Any

import pytest

from mysql_mimic import ColumnType
from mysql_mimic.errors import MysqlError
from mysql_mimic.results import ensure_result_set


async def gen_rows() -> Any:
    yield 1, None, None
    yield None, "2", None
    yield None, None, None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result, column_types",
    [
        (
            (gen_rows(), ["a", "b", "c"]),
            [ColumnType.LONGLONG, ColumnType.STRING, ColumnType.NULL],
        ),
    ],
)
async def test_ensure_result_set_columns(result: Any, column_types: Any) -> None:
    result_set = await ensure_result_set(result)
    assert [c.type for c in result_set.columns] == column_types


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result",
    [
        [1, 2],
        ([[1, 2]], ["a", "b"], ["a", "b"]),
    ],
)
async def test_ensure_result_set__invalid(result: Any) -> None:
    with pytest.raises(MysqlError):
        await ensure_result_set(result)
