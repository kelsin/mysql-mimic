import io
import contextlib

from mysql_mimic import version


def test_version() -> None:
    assert isinstance(version.__version__, str)


def test_main() -> None:
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        version.main("__main__")

    assert f"{version.__version__}\n" == out.getvalue()
