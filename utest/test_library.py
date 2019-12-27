import unittest
from unittest import mock

from MicrosoftDataLibrary import MicrosoftDataLibrary
from MicrosoftDataLibrary import DatabaseClient


class TestMicrosoftDataLibrary(unittest.TestCase):

    @mock.patch.object(DatabaseClient, '__init__', return_value=None)
    def test_connections(self, mock_db) -> None:

        lib = MicrosoftDataLibrary()
        self.assertEquals(0, lib.number_of_connections())

        with self.assertRaises(RuntimeError):
            lib.current_connection

        lib.connect(connection_name='conn1', connection_string='conn_url1')

        self.assertEquals(1, lib.number_of_connections())
        self.assertEquals(['conn1'], lib.list_connections())
        self.assertEquals('conn1', lib.current_connection_name())

        mock_db.assert_called_once_with(connection_string='conn_url1')

        conn1 = lib.current_connection

        lib.connect(connection_name='conn2', connection_string='conn_url2')
        self.assertEquals(2, lib.number_of_connections())

        self.assertNotEqual(conn1, lib.current_connection)
        conn2 = lib.current_connection

        self.assertEquals(['conn1', 'conn2'], lib.list_connections())
        self.assertEquals('conn2', lib.current_connection_name())
        self.assertEquals('conn2', lib.switch_connection('conn1'))

        self.assertEquals(conn1, lib.current_connection)
        self.assertEquals('conn1', lib.current_connection_name())

        lib.disconnect()

        self.assertEquals(['conn2'], lib.list_connections())

    def test_use_pandas(self) -> None:

        lib = MicrosoftDataLibrary(use_pandas=True)
        self.assertTrue(lib.use_pandas(False))
        self.assertFalse(lib.use_pandas(False))