import os

from pyspark.sql import SparkSession

from uc.management import UnityCatalog


class UnityCatalogNameConfig:
    """
    Configures and validates naming conventions for Unity Catalog entities such as business units, data layers, and environments.
    """
    def __init__(self, business_unit: str, data_layer: str, environment: str):
        self.allowed_business_units = {'elm', 'cts', 'finance', 'esg', 'amb_us', 'amb_eu', 'pnc'}
        self.allowed_data_layers = {'raw', 'curated'}
        self.allowed_environments = {'dev', 'uat', 'prod'}

        self.business_unit = self._validate_business_unit(business_unit)
        self.data_layer = self._validate_data_layer(data_layer)
        self.environment = self._validate_environment(environment)

    def _validate_business_unit(self, business_unit: str) -> str:
        if business_unit not in self.allowed_business_units:
            raise ValueError(f"Invalid business_unit '{business_unit}'. Allowed values are: {self.allowed_business_units}")
        return business_unit

    def _validate_data_layer(self, data_layer: str) -> str:
        if data_layer not in self.allowed_data_layers:
            raise ValueError(f"Invalid data_layer '{data_layer}'. Allowed values are: {self.allowed_data_layers}")
        return data_layer

    def _validate_environment(self, environment: str) -> str:
        if environment not in self.allowed_environments:
            raise ValueError(f"Invalid environment '{environment}'. Allowed values are: {self.allowed_environments}")
        return environment

class DatabricksContext:
    """
    Encapsulates the Databricks environment setup, providing Spark session and dbutils.
    """
    def __init__(self, spark: SparkSession):
        self.spark = spark
        if not self._is_running_on_databricks():
            raise EnvironmentError("This class is intended to run in a Databricks environment only.")
        self.dbutils = self._get_dbutils()

    def _is_running_on_databricks(self) -> bool:
        """Check if the code is running in a Databricks environment."""
        return 'DATABRICKS_RUNTIME_VERSION' in os.environ

    def _get_dbutils(self):
        try:
            from pyspark.dbutils import DBUtils
            return DBUtils(self.spark)
        except ImportError:
            # Fallback for Databricks notebook environment where dbutils is pre-defined
            return dbutils  # noqa: F821

class UnityCatalogApi:
    """
    Provides an interface to interact with Unity Catalog, enforcing naming configurations and offering utility methods.
    """
    def __init__(self, databricks_context: DatabricksContext, name_config: UnityCatalogNameConfig):
        self.databricks_context = databricks_context
        self.name_config = name_config

    def create_schema(self, catalog_name: str, schema_name: str):
        """Creates a schema (pre-fixed with the data layer) within a catalog, ensuring the catalog exists."""

        uc = UnityCatalog()
        if not uc.catalog_exists(catalog_name):
            raise ValueError(f"Catalog '{catalog_name}' does not exist.")
        full_schema_name = f'{self.name_config.data_layer}_{schema_name}'
        uc.create_schema(catalog_name, full_schema_name)
        
    def register_external_table(self, relative_adls_path: str, schema_name: str, table_name: str):
        """Registers an external table in Unity Catalog with a specified ADLS path."""

        base_adls_path = f'abfss://{self.name_config.data_layer}@dlsgde{self.name_config.environment}.dfs.core.windows.net/'
        catalog_name = f'{self.name_config.business_unit}_{self.name_config.environment}' if self.name_config.environment != 'prod' else self.name_config.business_unit
        unity_catalog_location = f'{catalog_name}.{schema_name}.{table_name}'
        self.databricks_context.spark.sql(f"CREATE TABLE IF NOT EXISTS {unity_catalog_location} LOCATION '{base_adls_path + relative_adls_path}'")

    def list_files(self, relative_path: str):
        """Lists files at a given path within the configured ADLS storage."""

        adls_base = f'abfss://{self.name_config.data_layer}@dlsgde{self.name_config.environment}.dfs.core.windows.net/'
        return self.databricks_context.dbutils.fs.ls(adls_base + relative_path)