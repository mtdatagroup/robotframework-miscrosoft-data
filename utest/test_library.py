import unittest
from unittest import mock
from unittest.mock import MagicMock
from pandas import DataFrame

from MicrosoftDataLibrary import MicrosoftDataLibrary
from MicrosoftDataLibrary import DatabaseClient


class TestMicrosoftDataLibrary(unittest.TestCase):

    def setUp(self) -> None:

        self.lib = MicrosoftDataLibrary()
        self.mock_connection = MagicMock()
        self.lib._connections = [self.mock_connection]
        self.lib._current_connection = self.mock_connection

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

    @mock.patch('MicrosoftDataLibrary.MicrosoftDataLibrary.read_query')
    def test_read_table(self, mock_query) -> None:

        mock_query.return_value = [{"count": "1"}]
        self.lib.read_table("a", "b")
        mock_query.assert_called_once_with(query='SELECT * FROM a.b')

    @mock.patch('MicrosoftDataLibrary.MicrosoftDataLibrary.read_query')
    def test_table_row_count(self, mock_query) -> None:

        mock_query.return_value = [{"count": "1"}]
        self.assertEquals(1, self.lib.table_row_count("a", "b"))
        mock_query.assert_called_once_with(query='SELECT COUNT(*) FROM a.b')

        mock_query.reset_mock()

        self.lib.use_pandas(True)
        mock_query.return_value = DataFrame([[1]], columns=['Count'])
        self.assertEquals(1, self.lib.table_row_count("a", "b"))
        mock_query.assert_called_once_with(query='SELECT COUNT(*) FROM a.b')

    @mock.patch('MicrosoftDataLibrary.MicrosoftDataLibrary.connect')
    def test_connect_with_trusted_config(self, mock_connect) -> None:

        config = {
            "trusted": "1",
            "dbname": "_dbname_",
            "hostname": "_hostname_",
            "dialect": "_dialect_",
            "driver": "_driver_"
        }
        self.lib.connect_with_config("conn1", config)
        expected_connection_string = '_dialect_://@_hostname_/_dbname_?driver=_driver_&trusted_connection=yes'
        mock_connect.assert_called_once_with(connection_name='conn1', connection_string=expected_connection_string)

    @mock.patch('MicrosoftDataLibrary.MicrosoftDataLibrary.connect')
    def test_connect_with_username_and_password_config(self, mock_connect) -> None:

        config = {
            "username": "_username_",
            "password": "_password_",
            "dbname": "_dbname_",
            "hostname": "_hostname_",
            "dialect": "_dialect_",
            "driver": "_driver_"
        }
        self.lib.connect_with_config("conn1", config)
        expected_connection_string = '_dialect_://_username_:_password_@_hostname_/_dbname_?driver=_driver_'
        mock_connect.assert_called_once_with(connection_name='conn1', connection_string=expected_connection_string)
