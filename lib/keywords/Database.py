from typing import List, Dict, Any

from robot.api.deco import keyword
from robot.api import logger
from database import api as db
import pandas as pd

ROBOT_LIBRARY_SCOPE = "TEST_SUITE"


class Database:

    def __init__(self, use_pandas: bool = False) -> None:
        self.__current_connection = None
        self.__connections = {}
        self.__use_pandas = use_pandas

    @property
    def current_connection(self) -> db.Api:
        if self.__current_connection is None:
            raise RuntimeError("No connection has been established")
        return self.__current_connection

    def use_pandas(self, use_pandas: bool) -> bool:
        self.__use_pandas = use_pandas

    @keyword
    def number_of_connections(self) -> int:
        return len(self.__connections)

    @keyword(types={"connection_name": str, "connection_string": str})
    def connect_to_mssql(self, connection_name: str, connection_string: str) -> None:
        from database import mssql
        self.__connections[connection_name] = mssql.MsSql(connection_string=connection_string)
        self.__current_connection = self.__connections[connection_name]

    @keyword
    def disconnect_all(self):
        for connection in self.__connections.values():
            connection.disconnect()
        self.__connections.clear()
        self.__current_connection = None

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
