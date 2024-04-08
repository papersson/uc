# Unity Catalog API Usage Guide

This guide provides examples on how to use the `UnityCatalogApi` class within the `uc/api/` module to interact with Unity Catalog in Databricks. The examples cover creating schemas, registering external tables, and listing files in Azure Data Lake Storage Gen2 (ADLS2) to register as Delta tables.

It consists of two components:

1. `DatabricksContext`: This class encapsulates the Databricks environment setup, providing Spark session and dbutils.
2. `UnityCatalogNameConfig`: This class provides a way to configure the name of the catalog, schema, and table. It allows for customization of the name prefixes and suffixes based on the business unit, data layer, and environment.

## Prerequisites

- The `uc` module must be installed on the cluster.

## Components
**DatabricksContext**:
The `DatabricksContext` class is designed to ensure that the API module is executed within a Databricks environment. It encapsulates the setup of the Databricks environment, providing access to the Spark session and dbutils. Additionally, it validates that the client code is running on Databricks by checking for the presence of a Databricks-specific environment variable.

**UnityCatalogNameConfig**:
The `UnityCatalogNameConfig` class is utilized to enforce naming standards for Unity Catalog entities such as catalogs, schemas, and tables. It ensures that names for these entities adhere to predefined conventions regarding business units, data layers, and environments.

**UnityCatalogApi**:
The `UnityCatalogApi` class is the main class for interacting with Unity Catalog. It encapsulates functionality for creating catalogs and schemas, and also uses the Spark session provided by the DatabricksContext to interact with ADLS2, which is needed to provide functionality for registering external tables and listing files in ADLS (useful if you want to register a folder of Delta Tables).

## Initialization

Before using the API, initialize the `UnityCatalogApi` with a `DatabricksContext` and `UnityCatalogNameConfig`:

```python
from uc.api import DatabricksContext, UnityCatalogNameConfig, UnityCatalogApi

databricks_context = DatabricksContext(spark)
name_config = UnityCatalogNameConfig(business_unit="elm", data_layer="curated", environment="dev")
uc_api = UnityCatalogApi(databricks_context, name_config)
```

### Workflow 1: Creating a Catalog

To create a schema within a catalog, ensuring the catalog exists:
```python
uc_api.create_catalog()
```

This will create a catalog named `elm_dev` if it does not exist. Note that the name of the catalog is fully determined by the `UnityCatalogNameConfig` class, specifically from its `business_unit` and `environment` properties. 

It will also automatically create three Databricks account-level groups: `elm_dev_read`, `elm_dev_write`, and `elm_dev_write_metadata`. For now members/groups will manually have to be added to these groups using the Databricks Account UI.

### Workflow 2: Creating a Schema

To create a schema within a catalog, ensuring the catalog exists:
```python
catalog_name = "elm_dev"
schema_name = "customer_satisfaction_denmark"
uc_api.create_schema(catalog_name, schema_name)
```

This will create a schema named `curated_customer_satisfaction_denmark` within the catalog `elm_dev` if it exists. Note that the resulting schema name is pre-fixed with the data layer during creation.

The API does not currently create schema-level group creation -- for now all of the access management is at the catalog granularity.

### Workflow 3: Registering an External Table

To register an external table in Unity Catalog with a specified ADLS path:
```python
relative_adls_path = "elm/customer_satisfaction/fact_responses"
schema_name = "curated_customer_satisfaction_denmark"
table_name = "fact_responses"
uc_api.register_external_table(relative_adls_path, schema_name, table_name)
```

This registers an external table named `fact_responses` within the `elm_dev.curated_customer_satisfaction_denmark` schema, pointing to the specified ADLS path.

### Workflow 4: Listing Files and Registering Delta Tables

To list files in a given ADLS path and register each as a Delta Table:
```python
relative_path = "elm/customer_satisfaction/denmark"
files = uc_api.list_files(relative_path)
for file in files:
    adls_delta_path = f'{relative_path}/{file.name}/delta_table'
    uc_api.register_external_table(adls_delta_path, "curated_customer_satisfaction_denmark", file.name)
```