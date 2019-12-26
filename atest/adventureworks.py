import copy
import os
from typing import Dict

if os.name == 'nt':
    _DB_DRIVER = "SQL+Server+Native+Client+11.0"
else:
    _DB_DRIVER = "ODBC+Driver+17+for+SQL+Server"

_DB_DIALECT = "mssql+pyodbc://"


def _dict_to_connection_string(args: Dict[str, str]) -> str:
    return f"{args['dialect']}{args['user']}:{args['pass']}@{args['host']}/{args['dbname']}?driver={args['driver']}"


db_config = {
    "user": "user",
    "pass": "pass",
    "dbname": "AdventureWorksDW2017",
    "host": os.environ.get("HOST", "localhost"),
    "dialect": _DB_DIALECT,
    "driver": _DB_DRIVER
}

connection_string = _dict_to_connection_string(db_config)

ssis_config = copy.deepcopy(db_config)
ssis_config["dbname"] = "SSISDB"

ssis_connection_string = _dict_to_connection_string(ssis_config)