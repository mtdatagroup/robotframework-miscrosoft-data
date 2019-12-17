from typing import List, Dict

import pandas as pd

from database import api as db, mssql


class SSISApi:

    DB_NAME = "SSISDB"

    def __init__(self, database: db.Api):
        self.__database = database

    @property
    def db_name(self) -> str:
        return self.DB_NAME

    @property
    def database(self) -> db.Api:
        return self.__database

    def __query_catalog(self) -> pd.DataFrame:
        query = """SELECT fd.name as 'folder_name', 
                          pj.name as 'project_name', 
                          pk.name as 'package_name'
                     FROM catalog.projects pj 
                     JOIN catalog.folders fd
                       ON pj.folder_id = fd.folder_id
                     JOIN catalog.packages pk
                       ON pj.project_id = pk.project_id
        """
        return self.__database.read_query(query)

    def list_catalog(self) -> List[Dict[str, str]]:
        return self.__query_catalog().to_dict(orient="records")

    def list_folders(self) -> List[str]:
        df = self.__query_catalog()
        return df['folder_name'].unique()

    def list_projects(self, folder_name: str) -> List[str]:
        df = self.__query_catalog()
        project_df = df[df["folder_name"] == folder_name]
        return project_df["project_name"].unique()

    def list_packages(self, folder_name: str, project_name: str) -> List[str]:
        df = self.__query_catalog()
        package_df = df[(df["folder_name"] == folder_name) & (df["project_name"] == project_name)]
        return package_df["package_name"].unique()

    def list_all_projects(self) -> List[str]:
        df = self.__query_catalog()
        return df['project_name'].unique()

    def list_all_packages(self) -> List[str]:
        df = self.__query_catalog()
        return df['package_name'].unique()

    @property
    def catalog_properties(self) -> Dict[str, str]:
        query = "select property_name, property_value from catalog.catalog_properties"
        df = self.__database.read_query(query)
        return {prop['property_name']: prop['property_value'] for prop in df.to_dict(orient="records")}

    def disconnect(self):
        self.database.disconnect()
        self.__database = None


if __name__ == "__main__":
    db_config = {
        "username": "user",
        "password": "pass",
        "dbname": SSISApi.DB_NAME,
        "host": "localhost"
    }

    connection_string = f"{db_config['username']}:{db_config['password']}@{db_config['host']}/{db_config['dbname']}"

    db = mssql.MsSql(connection_string=connection_string)

    ssis = SSISApi(database=db)

    print(ssis.catalog_properties)
    print(ssis.list_catalog())
    print(f"folders: {ssis.list_folders()}")
    print(f"projects: {ssis.list_all_projects()}")
    print(f"packages: {ssis.list_all_packages()}")

    print(f"projects[2]: {ssis.list_projects('AdventureWorksSSIS')}")
    print(f"packages[2]: {ssis.list_packages('AdventureWorksSSIS', 'ETL')}")