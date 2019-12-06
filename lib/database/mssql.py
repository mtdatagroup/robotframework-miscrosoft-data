import logging

from database.api import Api


class MsSql(Api):

    ALCHEMY_DRIVER = "ODBC+Driver+17+for+SQL+Server"
    ALCHEMY_DIALECT = "mssql+pyodbc://"

    def __init__(self, connection_string: str) -> None:

        _token = "&" if "?" in connection_string else "?"
        _connection_string = f"{self.ALCHEMY_DIALECT}{connection_string}{_token}driver={self.ALCHEMY_DRIVER}"
        super().__init__(connection_string=_connection_string)
        self._logger = logging.getLogger(self.__class__.__name__)