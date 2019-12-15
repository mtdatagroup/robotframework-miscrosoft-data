import os

db_config = {
    "username": "user",
    "password": "pass",
    "dbname": "AdventureWorksDW2017",
    "host": os.environ.get("HOST", "localhost")
}

connection_string = f"{db_config['username']}:{db_config['password']}@{db_config['host']}/{db_config['dbname']}"
