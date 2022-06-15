import unittest
from unittest.mock import Mock

from mysql_mimic import types
from mysql_mimic.connection import Connection


class TestMysqlServer(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = Connection(Mock(), Mock())

    def test_ok(self):
        self.conn.capabilities = self.conn.server_capabilities
        self.assertEqual(self.conn.ok(), b"\x00\x00\x00\x00\x00\x00\x00")

    def test_ok_eof(self):
        self.conn.capabilities = self.conn.server_capabilities
        self.assertEqual(self.conn.ok(eof=True), b"\xfe\x00\x00\x00\x00\x00\x00")

    def test_ok_non_41(self):
        self.assertEqual(self.conn.ok(), b"\x00\x00\x00")

    def test_ok_transactions(self):
        self.conn.capabilities = types.Capabilities.CLIENT_TRANSACTIONS
        self.assertEqual(self.conn.ok(), b"\x00\x00\x00\x00\x00")

    def test_eof(self):
        self.conn.capabilities = self.conn.server_capabilities
        self.assertEqual(self.conn.eof(), b"\xFE\x00\x00\x00\x00")

    def test_eof_non_41(self):
        self.assertEqual(self.conn.eof(), b"\xFE")

    def test_empty_error(self):
        self.conn.capabilities = self.conn.server_capabilities
        self.assertEqual(self.conn.error(msg=""), b"\xFFQ\x04#HY000")

    def test_empty_error_non_41(self):
        self.assertEqual(self.conn.error(msg=""), b"\xFFQ\x04")

    def test_simple_error(self):
        self.conn.capabilities = self.conn.server_capabilities
        self.assertEqual(self.conn.error(msg="kelsin"), b"\xFFQ\x04#HY000kelsin")

    def test_column_definition_41(self):
        self.assertEqual(
            self.conn.column_definition_41(),
            b"\x03def\x00\x00\x00\x00\x00\x0c!\x00\x00\x01\x00\x00\x0f\x00\x00\x00\x00\x00",
        )
