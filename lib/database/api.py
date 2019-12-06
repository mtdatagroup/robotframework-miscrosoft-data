import logging
from pprint import pprint
from typing import List

import pandas as pd
import sqlalchemy as alc


class Api:

    DEFAULT_CONNECTION_STRING = '{db_type}://{db_username}:{db_password}@{db_hostname}:{db_port}/{db_name}'

    def __init__(self, connection_string: str, **kwargs) -> None:
        self._engine = alc.create_engine(connection_string, **kwargs)
        self._logger = logging.getLogger(self.__class__.__name__)

    def __repr__(self):
        return str(self._engine)

    def disconnect(self):
        if callable(getattr(self._engine, "dispose", None)):
            self._engine.dispose()
        self._engine = None

    def execute_query(self, query: str) -> None:
        res = self._engine.execute(query)
        res.close()

    def read_query(self, query: str) -> pd.DataFrame:
        return pd.read_sql(query, con=self._engine)

    def load_df(self, df: pd.DataFrame, schema_name: str, table_name: str) -> None:
        df.to_sql(table_name, schema=schema_name, con=self._engine, index=False, if_exists='append')

    def list_schemas(self) -> List[str]:
        return alc.inspect(self._engine).get_schema_names()

    def list_tables(self, schema_name: str) -> List[str]:
        return self._engine.table_names(schema=schema_name)