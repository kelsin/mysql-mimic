from datetime import timezone, timedelta

import pytest

from mysql_mimic.errors import MysqlError
from mysql_mimic.variables import parse_timezone


def test_parse_timezone() -> None:
    assert timezone(timedelta()) == parse_timezone("UTC")
    assert timezone(timedelta()) == parse_timezone("+00:00")
    assert timezone(timedelta(hours=1)) == parse_timezone("+01:00")
    assert timezone(timedelta(hours=-1)) == parse_timezone("-01:00")

    # Implicitly test cache
    assert timezone(timedelta(hours=-1)) == parse_timezone("-01:00")

    with pytest.raises(MysqlError):
        parse_timezone("whoops")
