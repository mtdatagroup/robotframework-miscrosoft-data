import copy
import os
from typing import Dict


def __dict_to_connection_string(args: Dict[str, str]) -> str:
    return f"{args['username']}:{args['password']}@{args['host']}/{args['dbname']}"


db_config = {
    "username": "user",
    "password": "pass",
    "dbname": "AdventureWorksDW2017",
    "host": os.environ.get("HOST", "localhost")
}

connection_string = __dict_to_connection_string(db_config)

ssis_config = copy.deepcopy(db_config)
ssis_config["dbname"] = "SSISDB"

ssis_connection_string = __dict_to_connection_string(ssis_config)