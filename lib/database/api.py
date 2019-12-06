import logging
from pprint import pprint

import pandas as pd
import sqlalchemy as alc


class DatabaseApi:

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
        self._logger.info(f'executing query {query}')
        res = self._engine.execute(query)
        res.close()

    def read_query(self, query: str) -> pd.DataFrame:
        self._logger.info(f'executing query {query} and return result set as a pandas DataFrame')
        return pd.read_sql(query, con=self._engine)


if __name__ == "__main__":

    drivers={
        "win-dev": "SQL+Server+Native+Client+11.0",
        "centos7-dev": "ODBC+Driver+17+for+SQL+Server"
    }
    server="10.0.0.126"
    driver=drivers["centos7-dev"]

    conn = f"mssql+pyodbc://user:pass@{server}/AdventureWorksDW2017?driver={driver}"

    db = DatabaseApi(connection_string=conn)
    df = db.read_query("SELECT * FROM dbo.DimCustomer")
    pprint(df)
    db.disconnect()

    # conn = "mssql+pyodbc://localhost/AdventureWorksDW2017?driver=SQL+Server+Native+Client+11.0&trusted_connection=yes"
    # db = DatabaseApi(connection_string=conn)
    # df = db.read_query("SELECT * FROM dbo.DimCustomer")
    # pprint(df)
    # db.disconnect()