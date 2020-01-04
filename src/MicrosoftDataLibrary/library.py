import collections
from typing import List, Dict, Any

from robot.api import logger
from robot.api.deco import keyword
import pandas as pd
from .client import DatabaseClient, SSISClient
from .version import VERSION

__version__ = VERSION

Config = collections.namedtuple('Config', 'use_pandas ssis_server dtexec_path')


class MicrosoftDataLibrary:
    """MicrosoftDataLibrary is a Robot Framework library specific to SQL Server and SSIS

    """

    ROBOT_LIBRARY_SCOPE = 'GLOBAL'
    ROBOT_LIBRARY_VERSION = __version__

    _PANDA_DATAFRAME_ROW_COUNT_SHAPE = 0
    _PANDA_DATAFRAME_COL_COUNT_SHAPE = 1

    _DEFAULT_USE_PANDAS = False
    _DEFAULT_SSIS_SERVER = "localhost"
    _DEFAULT_DTEXEC_PATH = "dtexec"

    def __init__(self,
                 use_pandas: bool = _DEFAULT_USE_PANDAS,
                 ssis_server: str = _DEFAULT_SSIS_SERVER,
                 dtexec_path: str = _DEFAULT_DTEXEC_PATH) -> None:
        """MicrosoftDataLibrary allows some import time configuration to be set.

        The following parameters can be set:
        | = Parameter = | = Description =                                          | = Default = |
        | use_pandas    | if set to True, all results will be as a Pandas Datframe | $FALSE}     |
        | ssis_server   | hostname of SSIS server                                  | localhost   |
        | dtexec_path   | full path to dtexec binary                               | dtexec      |

        For example:
        | Library | MicrosoftDataLibrary |

        This will setup the library to return Pandas Dataframes:
        | Library | MicrosoftDataLibrary | ${TRUE} |

        You can also use named parameters:
        | Library | MicrosoftDataLibrary | use_pandas=${TRUE} | ssis_server=192.168.0.1 |
        """

        self._config = Config(
            use_pandas or self._DEFAULT_USE_PANDAS,
            ssis_server or self._DEFAULT_SSIS_SERVER,
            dtexec_path or self._DEFAULT_DTEXEC_PATH
        )

        self._current_connection = None
        self._connections = {}
        self._ssis_catalog_client = None
        self._ssis_exec_client = None

    @property
    def ssis_catalog_client(self) -> DatabaseClient:
        if self._ssis_catalog_client is None:
            raise RuntimeError("No connection to the SSIS Catalog been established")
        return self._ssis_catalog_client

    @property
    def ssis_exec_client(self) -> SSISClient:
        if self._ssis_exec_client is None:
            self._ssis_exec_client = SSISClient(self._config.ssis_server, self._config.dtexec_path)
        return self._ssis_exec_client

    @property
    def current_connection(self) -> DatabaseClient:
        if self._current_connection is None:
            raise RuntimeError("No connection has been established")
        return self._current_connection

    @keyword
    def number_of_connections(self) -> int:
        """Retrieve the number of registered connections"""
        return len(self._connections)

    @keyword(types={"connection_name": str, "connection_string": str})
    def connect(self, connection_name: str, connection_string: str) -> None:
        """Connect to SQL Server database instance.

        Multiple connections are possible, so a connection name is required to switch between them.
        """
        self._connections[connection_name] = DatabaseClient(connection_string=connection_string)
        self._current_connection = self._connections[connection_name]

    @keyword
    def connect_with_config(self, connection_name: str, config: Dict[str, str]):
        """Connect to SQL Server using a configuration dictionary

        Example of dictionary:

        | = Key =    | = Description =             | = Example =                   |
        | username   | SQL Authenticated username  | user                          |
        | password   | SQL Authenticated password  | secret_password               |
        | dbname     | Database name to login      | AdventureWorks                |
        | hostname   | Hostname of the SQL Server  | localhost                     |
        | dialect    | Alchemy dialect to use      | mssql+pyodbc                  |
        | driver     | Alchemy driver to use       | SQL+Server+Native+Client+11.0 |

        This will generate an Alchemy URL similar to:

            - `mssql+pyodbc://user:pAssword1@localhost/AdventureWorks?driver=SQL+Server+Native+Client+11.0`

        | = Key =    | = Description =             | = Example =                   |
        | trusted    | Use Trusted authentication  | Yes                           |
        | dbname     | Database name to login      | AdventureWorks                |
        | hostname   | Hostname of the SQL Server  | localhost                     |
        | dialect    | Alchemy dialect to use      | mssql+pyodbc                  |
        | driver     | Alchemy driver to use       | ODBC+Driver+17+for+SQL+Server |

        This will generate an Alchemy URL similar to:

            - `mssql+pyodbc://@localhost/AdventureWorks?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes`

        """
        try:

            _trusted = config.get("trusted", "0").lower() in ("yes", "true", "t", "1")

            alchemy_url = f"{config['dialect']}://"

            if _trusted is False:
                alchemy_url += f"{config['username']}:{config['password']}"

            alchemy_url += f"@{config['hostname']}/{config['dbname']}?driver={config['driver']}"

            if _trusted:
                alchemy_url += "&trusted_connection=yes"

            self.connect(connection_name=connection_name, connection_string=alchemy_url)

        except KeyError as e:
            logger.error(f"Missing configuration item {e}")
            raise RuntimeError

    @staticmethod
    def _table_select_statement(schema_name: str, table_name: str) -> str:
        return f"SELECT * FROM {schema_name}.{table_name}"

    @staticmethod
    def _table_select_count_statement(schema_name: str, table_name: str) -> str:
        return f"SELECT COUNT(*) FROM {schema_name}.{table_name}"

    @keyword(types={"connection_string": str})
    def connect_to_ssis_catalog(self, connection_string: str) -> None:
        """Connect to SSIS Database catalog (typically SSISDB).

        There can only be one connection at a time.
        """
        self._ssis_catalog_client = DatabaseClient(connection_string=connection_string)

    @keyword
    def disconnect_all(self) -> None:
        """Disconnect all registered connections, including any SSIS catalog connections"""
        for connection in self._connections.values():
            connection.disconnect()
        self._connections.clear()
        self._current_connection = None

        self.disconnect_from_ssis_catalog()

    @keyword
    def disconnect_from_ssis_catalog(self):
        """Disconnect from the SSIS Database catalog"""
        if self._ssis_catalog_client:
            self._ssis_catalog_client.disconnect()
            self._ssis_catalog_client = None

    @keyword
    def disconnect(self) -> None:
        """Disconnect current active connection"""
        try:
            self.current_connection.disconnect()
        except Exception as e:
            logger.error(e)
        finally:
            del self._connections[self.current_connection_name()]
            self._current_connection = None

    @keyword(types={"connection_name": str})
    def switch_connection(self, connection_name: str) -> str:
        """Switch active connection to connection name

        This will return the name of the active connection (if set)
        """
        if connection_name in self._connections:
            _current_connection_name = self.current_connection_name() if self._current_connection else ""
            self._current_connection = self._connections[connection_name]
            return _current_connection_name

        raise RuntimeError(f"Connection '{connection_name}' is not established in connection pool")

    @keyword
    def current_connection_name(self) -> str:
        """Get the current active connection name"""
        for k, v in self._connections.items():
            if v == self._current_connection:
                return k
        raise RuntimeError("No connection has been established")

    @keyword
    def list_connections(self) -> List[str]:
        """Get list of all registered connections."""
        return list(self._connections.keys())

    @keyword(types={"query": str})
    def execute_query(self, query: str) -> None:
        """Execute an SQL query"""
        self.current_connection.execute_query(query)

    @keyword(types={"schema_name": str, "table_name": str})
    def read_table(self, schema_name: str, table_name: str) -> Any:
        """Read all contents of table"""
        return self.read_query(query=self._table_select_statement(schema_name=schema_name, table_name=table_name))

    @keyword(types={"query": str})
    def read_query(self, query: str) -> Any:
        """Execute query and return result set"""
        df = self.current_connection.read_query(query)
        return df if self._config.use_pandas else df.to_dict(orient="records")

    @keyword(types={"query": str})
    def read_scalar(self, query: str) -> str:
        """Get single value back (first column from first record)"""
        res = self.current_connection.read_query(query=query)
        return res.iloc[0][0]

    @keyword
    def list_schemas(self) -> List[str]:
        """List all schemas"""
        return self.current_connection.list_schemas()

    @keyword(types={"schema_name": str})
    def list_tables(self, schema_name: str) -> List[str]:
        """List all tables"""
        return self.current_connection.list_tables(schema_name=schema_name)

    @keyword(types={"schema_name": str})
    def schema_exists(self, schema_name: str) -> bool:
        """Determine whether schema exists in database"""
        return schema_name in self.list_schemas()

    @keyword(types={"schema_name": str, "table_name": str})
    def table_exists(self, schema_name: str, table_name: str) -> bool:
        """Determine whether table exists in schema"""
        return table_name in self.list_tables(schema_name=schema_name)

    @keyword(types={"schema_name": str, "table_name": str})
    def table_is_empty(self, schema_name: str, table_name: str) -> bool:
        """Assert that a table contains no records"""
        return self.table_row_count(schema_name, table_name) <= 0

    @keyword(types={"schema_name": str, "table_name": str})
    def table_is_not_empty(self, schema_name: str, table_name: str) -> bool:
        """Assert that a table is not empty and contains records"""
        return self.table_row_count(schema_name, table_name) > 0

    @keyword(types={"schema_name": str, "table_name": str})
    def table_row_count(self, schema_name: str, table_name: str) -> int:
        """Get number of records in table"""
        return int(self.read_scalar(self._table_select_count_statement(schema_name=schema_name, table_name=table_name)))

    @keyword(types={"query": str})
    def query_row_count(self, query: str) -> int:
        """Get number of records from query"""
        df = self.current_connection.read_query(query)
        return df.shape[self._PANDA_DATAFRAME_ROW_COUNT_SHAPE]

    @keyword(types={"file_path": str, "sheet_name": str})
    def get_xlsx(self, file_path: str, sheet_name: str) -> pd.DataFrame:
        """Read contents of xlsx file into a Pandas Dataframe"""
        return pd.read_excel(file_path, sheet_name=sheet_name, index_col=None, header=0)

    @keyword(types={"expected_dataframe": pd.DataFrame, "actual_dataframe": pd.DataFrame})
    def dataframes_should_match(self, expected_dataframe: pd.DataFrame, actual_dataframe: pd.DataFrame):
        """Assert that expected and actual dataframes are equal"""
        if expected_dataframe.equals(actual_dataframe) is False:
            raise AssertionError("Actual does not match expected.")

    @keyword(types={"schema_name": str, "table_name": str, "file_path": str, "sheet_name": str})
    def table_should_match_xlsx(self, schema_name: str, table_name: str, file_path: str, sheet_name: str):
        """Assert that contents of database table match contents of XLSX"""
        xlsx_df = self.get_xlsx(file_path=file_path, sheet_name=sheet_name)
        table_df = self.current_connection.read_query(self._table_select_statement(schema_name, table_name))
        self.dataframes_should_match(xlsx_df, table_df)

    @keyword(types={"query": str, "file_path": str, "sheet_name": str})
    def query_should_match_xlsx(self, query: str, file_path: str, sheet_name: str):
        """Assert that the result set of a query matches the contents of XLSX"""
        xlsx_df = self.get_xlsx(file_path=file_path, sheet_name=sheet_name)
        table_df = self.current_connection.read_query(query)
        self.dataframes_should_match(xlsx_df, table_df)

    def _load_table_with_dataframe(self, df: pd.DataFrame, schema_name: str, table_name: str) -> int:
        self.current_connection.load_df(df=df, schema_name=schema_name, table_name=table_name)
        return self.table_row_count(schema_name=schema_name, table_name=table_name)

    @keyword(types={"schema_name": str, "table_name": str, "file_path": str})
    def load_table_with_csv(self, schema_name: str, table_name: str, file_path: str) -> int:
        """Append CSV to table and return the total number of records in the table"""
        df = pd.read_csv(filepath_or_buffer=file_path, header=0)
        return self._load_table_with_dataframe(df, schema_name, table_name)

    @keyword(types={"schema_name": str, "table_name": str, "file_path": str, "sheet_name": str})
    def load_table_with_xlsx(self, schema_name: str, table_name: str, file_path: str, sheet_name: str) -> int:
        """Append XLSX to table and return the total number of records in the table"""
        df = self.get_xlsx(file_path=file_path, sheet_name=sheet_name)
        return self._load_table_with_dataframe(df, schema_name, table_name)

    @keyword(types={"schema_name": str, "table_name": str})
    def get_table_metadata(self, schema_name: str, table_name: str) -> List[Dict[str, str]]:
        """Retrieve table information"""
        df = self.current_connection.get_table_metadata(schema_name=schema_name, table_name=table_name)
        return df.to_dict(orient="records")

    @keyword(types={"schema_name": str, "table_name": str})
    def truncate_table(self, schema_name: str, table_name: str) -> None:
        """Truncate a table"""
        self.current_connection.truncate_table(schema_name=schema_name, table_name=table_name)

    @keyword
    def list_functions(self) -> List[str]:
        """List all functions"""
        return self.current_connection.list_functions()

    @keyword
    def list_procedures(self) -> List[str]:
        """List all stored procedures"""
        return self.current_connection.list_procedures()

    @keyword(types={"procedure_name": str, "params": List[str]})
    def execute_procedure(self, procedure_name: str, params: List[str] = None) -> Any:
        """Execute a stored procedure"""
        return self.current_connection.execute_procedure(procedure_name=procedure_name, params=params)

    @keyword
    def get_ssis_catalog_properties(self) -> Dict[str, str]:
        """Retrieve all SSIS Catalog properties"""
        return self.ssis_catalog_client.get_ssis_catalog_properties()

    @keyword
    def list_ssis_folders(self) -> List[str]:
        """List all SSIS folders"""
        return self.ssis_catalog_client.list_ssis_folders()

    @keyword(types={"folder_name": str})
    def ssis_folder_exists(self, folder_name: str) -> bool:
        """Determine whether a SSIS folder exists"""
        return folder_name in self.list_ssis_folders

    @keyword(types={"folder_name": str})
    def list_ssis_projects(self, folder_name: str) -> List[str]:
        """List all SSIS projects in a given folder"""
        return self.ssis_catalog_client.list_ssis_projects(folder_name)

    @keyword(types={"project_name": str, "folder_name": str})
    def ssis_project_exists(self, project_name: str, folder_name: str = None) -> bool:
        """Determine whether a SSIS project exists"""
        if folder_name is None:
            return project_name in self.list_all_ssis_projects()
        return project_name in self.list_ssis_projects(folder_name)

    @keyword(types={"folder_name": str, "project_name": str})
    def list_ssis_packages(self, folder_name: str, project_name: str) -> List[str]:
        """List all SSIS packages in a given folder and project"""
        return self.ssis_catalog_client.list_ssis_packages(folder_name=folder_name, project_name=project_name)

    @keyword
    def list_all_ssis_projects(self) -> List[str]:
        """List all SSIS projects"""
        return self.ssis_catalog_client.list_all_ssis_projects()

    @keyword
    def list_all_ssis_packages(self) -> List[str]:
        """List all SSIS packages"""
        return self.ssis_catalog_client.list_all_ssis_packages()

    @keyword(types={"package_path": str})
    def execute_ssis_package(self, package_path: str) -> int:
        """Execute a SSIS package stored on the Server"""

        completed_process = self.ssis_exec_client.execute_server_package(package_path)

        if completed_process.stdout:
            logger.info(completed_process.stdout)
        if completed_process.stderr:
            logger.error(completed_process.stderr)

        rc = completed_process.returncode

        logger.info(f"Return Code: {rc} - {SSISClient.RETURN_CODES[rc]}")

        return completed_process.returncode
