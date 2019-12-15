import os
import platform
from pprint import pprint
from typing import Dict

import pandas as pd
import sqlalchemy as alc

ALCHEMY_DRIVERS ={
    "win-dev": "SQL+Server+Native+Client+11.0",
    "container": "ODBC+Driver+17+for+SQL+Server"
}

WINDOWS_PLATFORM = platform.platform().startswith("Windows")

connection_args = {
    "user": "user",
    "pass": "pass",
    "dbname": "AdventureWorksDW2017",
    "host": os.environ.get("HOST", "localhost"),
    "dialect": "mssql+pyodbc://",
    "driver": ALCHEMY_DRIVERS["win-dev"] if WINDOWS_PLATFORM else ALCHEMY_DRIVERS["container"]
}

pprint(connection_args)


def connect(args: Dict[str, str]) -> str:
    conn_url = f"{args['dialect']}{args['user']}:{args['pass']}@{args['host']}/{args['dbname']}?driver={args['driver']}"
    print(f"Connection String: {conn_url}")
    return alc.create_engine(conn_url)


if __name__ == "__main__":
    engine = connect(connection_args)
    query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES"
    print(pd.read_sql(query, con=engine))
