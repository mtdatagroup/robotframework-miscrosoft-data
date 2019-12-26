from typing import List, Any, Dict
import pandas as pd
import sqlalchemy as alc
from sqlalchemy.orm import sessionmaker


class _ClientConfig:
    DB_DRIVER = "ODBC+Driver+17+for+SQL+Server"
    DB_DIALECT = "mssql+pyodbc://"
    SSIS_DB_NAME = "SSISDB"


class DatabaseClient:

    ALCHEMY_DRIVER = "ODBC+Driver+17+for+SQL+Server"
    ALCHEMY_DIALECT = "mssql+pyodbc://"
    SSIS_DB_NAME = "SSISDB"

    def __init__(self, connection_string: str, **kwargs) -> None:

        _token = "&" if "?" in connection_string else "?"
        _connection_string = f"{_ClientConfig.DB_DIALECT}{connection_string}{_token}driver={_ClientConfig.DB_DRIVER}"
        self._engine = alc.create_engine(connection_string, **kwargs)

    def __repr__(self):
        return str(self._engine)

    def disconnect(self):
        if callable(getattr(self._engine, "dispose", None)):
            self._engine.dispose()
        self._engine = None

    def execute_query(self, query: str) -> None:
        res = self._engine.execute(query)
        res.close()

    def read_query(self, query: str) -> pd.DataFrame:
        return pd.read_sql(query, con=self._engine)

    def load_df(self, df: pd.DataFrame, schema_name: str, table_name: str) -> None:
        df.to_sql(table_name, schema=schema_name, con=self._engine, index=False, if_exists='append')

    def truncate_table(self, schema_name: str, table_name: str) -> None:
        session_maker = sessionmaker(bind=self._engine)
        session = session_maker()
        session.execute(f"TRUNCATE TABLE {schema_name}.{table_name}")
        session.commit()
        session.close()

    def list_schemas(self) -> List[str]:
        return alc.inspect(self._engine).get_schema_names()

    def list_tables(self, schema_name: str) -> List[str]:
        return self._engine.table_names(schema=schema_name)

    def get_table_metadata(self, schema_name: str, table_name: str) -> pd.DataFrame:
        res = alc.inspect(self._engine).get_columns(schema=schema_name, table_name=table_name)
        return pd.DataFrame(res)

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

    def __query_ssis_catalog(self) -> pd.DataFrame:
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

    def list_ssis_catalog(self) -> List[Dict[str, str]]:
        return self.__query_ssis_catalog().to_dict(orient="records")

    def list_ssis_folders(self) -> List[str]:
        df = self.__query_ssis_catalog()
        return df['folder_name'].unique()

    def list_ssis_projects(self, folder_name: str) -> List[str]:
        df = self.__query_ssis_catalog()
        project_df = df[df["folder_name"] == folder_name]
        return project_df["project_name"].unique()

    def list_ssis_packages(self, folder_name: str, project_name: str) -> List[str]:
        df = self.__query_ssis_catalog()
        package_df = df[(df["folder_name"] == folder_name) & (df["project_name"] == project_name)]
        return package_df["package_name"].unique()

    def list_all_ssis_projects(self) -> List[str]:
        df = self.__query_ssis_catalog()
        return df['project_name'].unique()

    def list_all_ssis_packages(self) -> List[str]:
        df = self.__query_ssis_catalog()
        return df['package_name'].unique()

    @property
    def ssis_catalog_properties(self) -> Dict[str, str]:
        query = "select property_name, property_value from catalog.catalog_properties"
        df = self.__database.read_query(query)
        return {prop['property_name']: prop['property_value'] for prop in df.to_dict(orient="records")}
