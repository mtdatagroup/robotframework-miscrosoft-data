from typing import List, Dict, Any

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

    def __init__(self, use_pandas: bool = False) -> None:
        self.__current_connection = None
        self.__connections = {}
        self.__use_pandas = use_pandas
        self.__ssis_catalog_client = None

    @property
    def ssis_catalog_client(self) -> DatabaseClient:
        if self.__ssis_catalog_client is None:
            raise RuntimeError("No connection to the SSIS Catalog been established")
        return self.__ssis_catalog_client

    @property
    def current_connection(self) -> DatabaseClient:
        if self.__current_connection is None:
            raise RuntimeError("No connection has been established")
        return self.__current_connection

    def use_pandas(self, use_pandas: bool) -> bool:
        self.__use_pandas = use_pandas

    @keyword
    def number_of_connections(self) -> int:
        """Retrieve the number of registered connections
        """
        return len(self.__connections)

    @keyword(types={"connection_name": str, "connection_string": str})
    def connect(self, connection_name: str, connection_string: str) -> None:
        self.__connections[connection_name] = DatabaseClient(connection_string=connection_string)
        self.__current_connection = self.__connections[connection_name]

    @keyword(types={"connection_string": str})
    def connect_to_ssis_catalog(self, connection_string: str) -> None:
        self.__ssis_catalog_client = DatabaseClient(connection_string=connection_string)

    @keyword
    def disconnect_all(self):

        for connection in self.__connections.values():
            connection.disconnect()
        self.__connections.clear()
        self.__current_connection = None

        self.disconnect_from_ssis_catalog()

    @keyword
    def disconnect_from_ssis_catalog(self):

        if self.__ssis_catalog_client:
            self.__ssis_catalog_client.disconnect()
            self.__ssis_catalog_client = None

    @keyword
    def disconnect(self) -> None:
        self.current_connection.disconnect()
        del self.__connections[self.current_connection_name()]
        self.__current_connection = None

    @keyword(types={"connection_name": str})
    def switch_connection(self, connection_name: str) -> None:
        if connection_name in self.__connections:
            self.__current_connection = self.__connections[connection_name]
        else:
            raise RuntimeError(f"Connection '{connection_name}' is not established in connection pool")

    @keyword
    def current_connection_name(self) -> str:
        for k, v in self.__connections.items():
            if v == self.__current_connection:
                return k
        raise RuntimeError("No connection has been established")

    @keyword
    def list_connections(self) -> List[str]:
        return self.__connections.keys()

    @keyword(types={"query": str})
    def execute_query(self, query: str) -> None:
        self.current_connection.execute_query(query)

    @keyword(types={"schema_name": str, "table_name": str})
    def read_table(self, schema_name: str, table_name: str) -> Any:
        query = f"SELECT * FROM {schema_name}.{table_name}"
        return self.read_query(query=query)

    @keyword(types={"query": str})
    def read_query(self, query: str) -> Any:
        df = self.current_connection.read_query(query)
        return df if self.__use_pandas else df.to_dict(orient="records")

    @keyword(types={"query": str})
    def read_scalar(self, query: str) -> str:
        res = self.read_query(query=query)
        return res.iloc[0][0] if self.__use_pandas else list(res[0].values())[0]

    @keyword
    def list_schemas(self) -> List[str]:
        return self.current_connection.list_schemas()

    @keyword(types={"schema_name": str})
    def list_tables(self, schema_name: str) -> List[str]:
        return self.current_connection.list_tables(schema_name=schema_name)

    @keyword(types={"schema_name": str})
    def schema_exists(self, schema_name: str) -> bool:
        return schema_name in self.list_schemas()

    @keyword(types={"schema_name": str, "table_name": str})
    def table_exists(self, schema_name: str, table_name: str) -> bool:
        return table_name in self.list_tables(schema_name=schema_name)

    @keyword(types={"schema_name": str, "table_name": str})
    def row_count(self, schema_name: str, table_name: str) -> int:
        return int(self.read_scalar(f"SELECT COUNT(*) FROM {schema_name}.{table_name}"))

    @keyword(types={"file_path": str, "schema_name": str, "table_name": str})
    def load_table_with_csv(self, file_path: str, schema_name: str, table_name: str) -> int:
        df = pd.read_csv(filepath_or_buffer=file_path, header=0)
        self.current_connection.load_df(df=df, schema_name=schema_name, table_name=table_name)
        return self.row_count(schema_name=schema_name, table_name=table_name)

    @keyword(types={"schema_name": str, "table_name": str})
    def get_table_metadata(self, schema_name: str, table_name: str) -> List[Dict[str, str]]:
        df = self.current_connection.get_table_metadata(schema_name=schema_name, table_name=table_name)
        return df.to_dict(orient="records")

    @keyword(types={"schema_name": str, "table_name": str})
    def truncate_table(self, schema_name: str, table_name: str) -> None:
        self.current_connection.truncate_table(schema_name=schema_name, table_name=table_name)

    @keyword
    def list_functions(self) -> List[str]:
        return self.current_connection.list_functions()

    @keyword
    def list_procedures(self) -> List[str]:
        return self.current_connection.list_procedures()

    @keyword(types={"procedure_name": str, "params": List[str]})
    def execute_procedure(self, procedure_name: str, params: List[str] = None) -> Any:
        return self.current_connection.execute_procedure(procedure_name=procedure_name, params=params)

    @keyword
    def get_ssis_catalog_properties(self) -> Dict[str, str]:
        return self.ssis_catalog_client.get_ssis_catalog_properties()

    @keyword
    def list_ssis_folders(self) -> List[str]:
        return self.ssis_catalog_client.list_ssis_folders()

    @keyword(types={"folder_name": str})
    def list_ssis_projects(self, folder_name: str) -> List[str]:
        return self.ssis_catalog_client.list_ssis_projects(folder_name)

    @keyword(types={"folder_name": str, "project_name": str})
    def list_ssis_packages(self, folder_name: str, project_name: str) -> List[str]:
        return self.ssis_catalog_client.list_ssis_packages(folder_name=folder_name, project_name=project_name)

    @keyword
    def list_all_ssis_projects(self) -> List[str]:
        return self.ssis_catalog_client.list_all_ssis_projects()

    @keyword
    def list_all_ssis_packages(self) -> List[str]:
        return self.ssis_catalog_client.list_all_ssis_packages()