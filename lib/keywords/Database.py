from typing import List, Dict, Any

from robot.api.deco import keyword
from robot.api import logger
from database import api as db
import pandas as pd

ROBOT_LIBRARY_SCOPE = "TEST_SUITE"


class Database:

    def __init__(self):
        self.__current_connection = None
        self.__connections = {}

    @property
    def current_connection(self) -> db.Api:
        if self.__current_connection is None:
            raise RuntimeError("No connection has been established")
        return self.__current_connection

    @keyword
    def number_of_connections(self) -> int:
        return len(self.__connections)

    @keyword(types={"connection_name": str, "connection_string": str})
    def connect_to_mssql(self, connection_name: str, connection_string: str) -> None:
        from database import mssql
        self.__connections[connection_name] = mssql.MsSql(connection_string=connection_string)
        self.__current_connection = self.__connections[connection_name]

    @keyword
    def disconnect(self) -> None:

        def retrieve_name():
            for k, v in self.__connections.items():
                if v == self.__current_connection:
                    return k

        self.current_connection.disconnect()
        connection_name = retrieve_name()
        del self.__connections[connection_name]
        self.__current_connection = None

    @keyword(types={"connection_name": str})
    def switch_connection(self, connection_name: str) -> None:
        if connection_name in self.__connections:
            self.__current_connection = self.__connections[connection_name]
        else:
            raise RuntimeError(f"Connection {connection_name} is not established in connection pool")

    @keyword(types={"query": str})
    def execute_query(self, query: str) -> None:
        self.current_connection.execute_query(query)

    @keyword(types={"query": str, "as_pandas": bool})
    def read_query(self, query: str, as_pandas: bool = False) -> Any:
        df = self.current_connection.read_query(query)
        return df if as_pandas else df.to_dict(orient="records")

    @keyword(types={"query": str})
    def read_scalar(self, query: str) -> str:
        res = self.read_query(query=query, as_pandas=True)
        return res.iloc[0][0]

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
        self.execute_query(f"TRUNCATE TABLE {schema_name}.{table_name}")