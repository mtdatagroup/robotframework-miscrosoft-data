import logging

from database.api import DatabaseApi


class MsSql(DatabaseApi):

    ALCHEMY_DRIVER = "SQL+Server+Native+Client+11.0"

    def __init__(self, connection_string: str) -> None:
        super().__init__(connection_string=connection_string)
        self._logger = logging.getLogger(self.__class__.__name__)