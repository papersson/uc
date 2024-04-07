import os
from pyspark.sql import SparkSession

class NamingStandard:
    def __init__(self, business_unit: str, data_layer: str, environment: str):
        self.allowed_business_units = {'elm', 'cts', 'finance', 'esg', 'amb_us', 'amb_eu', 'pnc'}
        self.allowed_data_layers = {'raw', 'curated'}
        self.allowed_environments = {'dev', 'uat', 'prod'}

        self.business_unit = self.validate_business_unit(business_unit)
        self.data_layer = self.validate_data_layer(data_layer)
        self.environment = self.validate_environment(environment)

    def validate_business_unit(self, business_unit: str) -> str:
        if business_unit not in self.allowed_business_units:
            raise ValueError(f"Invalid business_unit '{business_unit}'. Allowed values are: {self.allowed_business_units}")
        return business_unit

    def validate_data_layer(self, data_layer: str) -> str:
        if data_layer not in self.allowed_data_layers:
            raise ValueError(f"Invalid data_layer '{data_layer}'. Allowed values are: {self.allowed_data_layers}")
        return data_layer

    def validate_environment(self, environment: str) -> str:
        if environment not in self.allowed_environments:
            raise ValueError(f"Invalid environment '{environment}'. Allowed values are: {self.allowed_environments}")
        return environment

class UnityCatalogApi:
    def __init__(self, spark: SparkSession, naming_standard: NamingStandard):
        self.spark = spark
        self.naming_standard = naming_standard

        if not self.is_running_on_databricks():
            raise EnvironmentError("This class is intended to run in a Databricks environment only.")
        
        self.dbutils = self.get_dbutils()

    def is_running_on_databricks(self):
        """Check if the code is running in a Databricks environment."""
        return 'DATABRICKS_RUNTIME_VERSION' in os.environ

    def get_dbutils(self):
        try:
            from pyspark.dbutils import DBUtils
            return DBUtils(self.spark)
        except ImportError:
            # Fallback for Databricks notebook environment where dbutils is pre-defined
            return dbutils  # noqa: F821
    
    def create_schema(self, catalog_name: str, schema_name: str):
        from uc.management import UnityCatalog
        uc = UnityCatalog()
        if not uc.catalog_exists(catalog_name):
            raise ValueError(f"Catalog '{catalog_name}' does not exist.")
        full_schema_name = f'{self.naming_standard.data_layer}_{schema_name}'
        uc.create_schema(catalog_name, full_schema_name)
        
    def register_external_table(self, relative_adls_path: str, schema_name: str, table_name: str):
        base_adls_path = f'abfss://{self.naming_standard.data_layer}@dlsgde{self.naming_standard.environment}.dfs.core.windows.net/'
        catalog_name = f'{self.naming_standard.business_unit}_{self.naming_standard.environment}' if self.naming_standard.environment != 'prod' else self.naming_standard.business_unit
        unity_catalog_location = f'{catalog_name}.{self.naming_standard.data_layer}_{schema_name}.{table_name}'
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {unity_catalog_location} LOCATION '{base_adls_path + relative_adls_path}'")

    def list_files(self, path: str):
        adls_base = f'abfss://{self.naming_standard.data_layer}@dlsgde{self.naming_standard.environment}.dfs.core.windows.net/'
        return self.dbutils.fs.ls(adls_base + path)