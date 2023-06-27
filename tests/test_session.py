from datetime import timezone, timedelta

import pytest

from mysql_mimic import Session
from mysql_mimic.errors import MysqlError


def test_parse_timezone() -> None:
    session = Session()
    assert timezone(timedelta()) == session.parse_timezone("UTC")
    assert timezone(timedelta()) == session.parse_timezone("+00:00")
    assert timezone(timedelta(hours=1)) == session.parse_timezone("+01:00")
    assert timezone(timedelta(hours=-1)) == session.parse_timezone("-01:00")

    with pytest.raises(MysqlError):
        session.parse_timezone("whoops")
