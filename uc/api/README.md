# Unity Catalog API Usage Guide

This guide provides examples on how to use the `UnityCatalogApi` class within the `uc/api/api.py` module to interact with Unity Catalog in Databricks. The examples cover creating schemas, registering external tables, and listing files in Azure Data Lake Storage Gen2 (ADLS2) to register as Delta tables.

## Prerequisites

- Databricks environment setup with access to Unity Catalog and ADLS2.
- The `uc` module must be installed on the cluster.
- The catalogs are assumed to have been created.

## Initialization

Before using the API, initialize the `UnityCatalogApi` with a `DatabricksContext` and `UnityCatalogNameConfig`:

```python
from pyspark.sql import SparkSession
from uc.api.api import DatabricksContext, UnityCatalogNameConfig, UnityCatalogApi

databricks_context = DatabricksContext(spark)
name_config = UnityCatalogNameConfig(business_unit="elm", data_layer="curated", environment="dev")
uc_api = UnityCatalogApi(databricks_context, name_config)
```


### Workflow 1: Creating a Schema

To create a schema within a catalog, ensuring the catalog exists:
```python
catalog_name = "elm_dev"
schema_name = "customer_satisfaction_denmark"
uc_api.create_schema(catalog_name, schema_name)
```

This will create a schema named `curated_customer_satisfaction_denmark` within the catalog `elm_dev` if it exists.

### Workflow 2: Registering an External Table

To register an external table in Unity Catalog with a specified ADLS path:
```python
relative_adls_path = "elm/customer_satisfaction/fact_responses"
schema_name = "curated_customer_satisfaction_denmark"
table_name = "fact_responses"
uc_api.register_external_table(relative_adls_path, schema_name, table_name)
```

This registers an external table named `fact_responses` within the `elm_dev.curated_customer_satisfaction_denmark` schema, pointing to the specified ADLS path.

### Example 3: Listing Files and Registering Delta Tables

To list files in a given ADLS path and register each as a Delta Table:
```python
relative_path = "elm/customer_satisfaction/denmark"
files = uc_api.list_files(relative_path)
for file in files:
    adls_delta_path = f'{relative_path}/{file.name}/delta_table'
    uc_api.register_external_table(adls_delta_path, "curated_customer_satisfaction_denmark", file.name)
```