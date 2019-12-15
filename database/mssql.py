import logging
from typing import List, Any

import pandas as pd
from sqlalchemy.exc import ProgrammingError

from database.api import Api


class MsSql(Api):
    ALCHEMY_DRIVER = "ODBC+Driver+17+for+SQL+Server"
    ALCHEMY_DIALECT = "mssql+pyodbc://"

    def __init__(self, connection_string: str) -> None:

        _token = "&" if "?" in connection_string else "?"
        _connection_string = f"{self.ALCHEMY_DIALECT}{connection_string}{_token}driver={self.ALCHEMY_DRIVER}"
        super().__init__(connection_string=_connection_string)
        self._logger = logging.getLogger(self.__class__.__name__)

    def list_functions(self) -> List[str]:
        query = "SELECT routine_name FROM information_schema.routines WHERE routine_type = 'FUNCTION'"
        df = self.read_query(query)
        return df['routine_name'].values

    def list_procedures(self) -> List[str]:
        query = "SELECT routine_name FROM information_schema.routines WHERE routine_type = 'PROCEDURE'"
        df = self.read_query(query)
        return df['routine_name'].values

    def execute_procedure(self, procedure_name: str, params: List[Any] = None) -> Any:

        if params:
            q_params = ",".join("?" * len(params))
            results_set = self._engine.execute(f"exec {procedure_name} {q_params}", *params)
        else:
            results_set = self._engine.execute(f"exec {procedure_name}")

        if results_set.returns_rows:
            return pd.DataFrame(results_set)
        return None
