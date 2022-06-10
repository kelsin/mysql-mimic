import io
import contextlib
import unittest

from mysql_mimic import version


class TestTypes(unittest.TestCase):
    def test_version(self):
        self.assertTrue(isinstance(version.__version__, str))

    def test_main(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            version.main("__main__")

        self.assertEqual(f"{version.__version__}\n", out.getvalue())
