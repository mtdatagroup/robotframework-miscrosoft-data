from typing import List, Dict, Any

from robot.api import logger
from robot.api.deco import keyword
import pandas as pd
from .client import DatabaseClient
from .version import VERSION

__version__ = VERSION


class MicrosoftDataLibrary:
    """MicrosoftDataLibrary is a Robot Framework library specific to SQL Server and SSIS

    """

    ROBOT_LIBRARY_SCOPE = 'GLOBAL'
    ROBOT_LIBRARY_VERSION = __version__

    _PANDA_DATAFRAME_ROW_COUNT_SHAPE = 0
    _PANDA_DATAFRAME_COL_COUNT_SHAPE = 1

    def __init__(self, use_pandas: bool = False) -> None:
        """use_pandas = True will return all query results as a Panda Dataframe"""
        self._current_connection = None
        self._connections = {}
        self._use_pandas = use_pandas
        self._ssis_catalog_client = None

    @property
    def ssis_catalog_client(self) -> DatabaseClient:
        if self._ssis_catalog_client is None:
            raise RuntimeError("No connection to the SSIS Catalog been established")
        return self._ssis_catalog_client

    @property
    def current_connection(self) -> DatabaseClient:
        if self._current_connection is None:
            raise RuntimeError("No connection has been established")
        return self._current_connection

    @keyword(types={"use_pandas": bool})
    def use_pandas(self, use_pandas: bool) -> bool:
        """Specify whether to return queries as a Panda Dataframe or Python Dictionary.

        This will return the current setting.
        """
        _orig = self._use_pandas
        self._use_pandas = use_pandas
        return _orig

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
        | password   | SQL Authencticated password | pAssword1                     |
        | dbname     | Database name to login      | AdventureWorks                |
        | hostname   | Hostname of the SQL Server  | localhost                     |
        | dialect    | Alchemy dialect to use      | mssql+pyodbc                  |
        | driver     | Alchemy driver to use       | SQL+Server+Native+Client+11.0 |

        This will generate an Alchemy URL similar to:

            mssql+pyodbc://user:pAssword1@localhost/AdventureWorks?driver=SQL+Server+Native+Client+11.0

        | = Key =    | = Description =             | = Example =                   |
        | trusted    | Use Trusted authentication  | Yes                           |
        | dbname     | Database name to login      | AdventureWorks                |
        | hostname   | Hostname of the SQL Server  | localhost                     |
        | dialect    | Alchemy dialect to use      | mssql+pyodbc                  |
        | driver     | Alchemy driver to use       | ODBC+Driver+17+for+SQL+Server |

        This will generate an Alchemy URL similar to:

            mssql+pyodbc://@localhost/AdventureWorks?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes

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
        except Exception:
            pass
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
        else:
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
        query = f"SELECT * FROM {schema_name}.{table_name}"
        return self.read_query(query=query)

    @keyword(types={"query": str})
    def read_query(self, query: str) -> Any:
        """Execute query and return result set"""
        df = self.current_connection.read_query(query)
        return df if self._use_pandas else df.to_dict(orient="records")

    @keyword(types={"query": str})
    def read_scalar(self, query: str) -> str:
        """Get single value back (first column from first record)"""
        res = self.read_query(query=query)
        return res.iloc[0][0] if self._use_pandas else list(res[0].values())[0]

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
    def table_row_count(self, schema_name: str, table_name: str) -> int:
        """Get number of records in table"""
        return int(self.read_scalar(f"SELECT COUNT(*) FROM {schema_name}.{table_name}"))

    @keyword(types={"query": str})
    def query_row_count(self, query: str) -> int:
        """Get number of records from query"""
        df = self.current_connection.read_query(query)
        return df.shape[self._PANDA_DATAFRAME_ROW_COUNT_SHAPE]

    @keyword(types={"file_path": str, "schema_name": str, "table_name": str})
    def load_table_with_csv(self, file_path: str, schema_name: str, table_name: str) -> int:
        """Append CSV to table and return the total number of records in the table"""
        df = pd.read_csv(filepath_or_buffer=file_path, header=0)
        self.current_connection.load_df(df=df, schema_name=schema_name, table_name=table_name)
        return self.table_row_count(schema_name=schema_name, table_name=table_name)

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
    def ssis_project_exists(self, project_name: str, folder_name: str = None) -> List[str]:
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