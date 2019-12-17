from typing import Dict, List

from SSHLibrary.deco import keyword

import mssql
from etl import ssis

ROBOT_LIBRARY_SCOPE = "TEST_SUITE"


class SSISLibrary:

    def __init__(self) -> None:
        self.__ssis_connection = None

    @property
    def ssis_connection(self) -> ssis.SSIS:
        if self.__ssis_connection is None:
            raise RuntimeError("No SSIS connection has been established")
        return self.__ssis_connection

    @keyword(types={"connection_string" : str})
    def connect(self, connection_string: str) -> None:
        db_api = mssql.MsSql(connection_string=connection_string)
        self.__ssis_connection = ssis.SSIS(db_api)

    @keyword
    def list_catalog_properties(self) -> Dict[str, str]:
        return self.ssis_connection.catalog_properties

    @keyword
    def list_folders(self) -> List[str]:
        return self.ssis_connection.list_folders()

    @keyword(types={"folder_name": str})
    def list_projects(self, folder_name: str) -> List[str]:
        return self.ssis_connection.list_projects(folder_name)

    @keyword(types={"folder_name": str, "project_name": str})
    def list_packages(self, folder_name: str, project_name: str) -> List[str]:
        return self.ssis_connection.list_packages(folder_name=folder_name, project_name=project_name)

    @keyword
    def list_all_projects(self) -> List[str]:
        return self.ssis_connection.list_all_projects()

    @keyword
    def list_all_packages(self) -> List[str]:
        return self.ssis_connection.list_all_packages()

    @keyword
    def disconnect(self) -> None:
        self.ssis_connection.disconnect()
