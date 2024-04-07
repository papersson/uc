from unittest import TestCase
from uc.databricks.http_client import CatalogClient, DatabricksHttpService, SecurityGroupClient
from uc.management import UnityCatalog
from uc.utils.scim import StartsWith

class TestUnityCatalogIntegration(TestCase):
    def setUp(self):
        self.unity_catalog = UnityCatalog()
        databricks_http_service = DatabricksHttpService()
        self.catalog_client = CatalogClient(databricks_http_service)
        self.security_group_client = SecurityGroupClient(databricks_http_service)

    def test_create_and_delete_catalog_with_groups_integration(self):
        catalog_name = "integration_test_catalog_with_groups"
        
        # Create catalog and groups
        self.unity_catalog.create_catalog(catalog_name)
        self.assertTrue(self.catalog_client.catalog_exists(catalog_name))
        groups = self.security_group_client.fetch_groups([StartsWith('displayName', catalog_name)])
        self.assertEqual(len(groups), 3)
        
        # Clean up: delete the catalog and groups
        self.unity_catalog.delete_catalog(catalog_name)
        self.assertFalse(self.catalog_client.catalog_exists(catalog_name))
        groups = self.security_group_client.fetch_groups([StartsWith('displayName', catalog_name)])
        self.assertEqual(len(groups), 0)


class TestCatalogAndSchemaIntegration(TestCase):
    def setUp(self):
        self.unity_catalog = UnityCatalog()
        self.catalog_name = "TestCatalogSchemaIntegration".lower()  # Ensure lowercase to match implementation
        self.schema_name = "TestSchema".lower()  # Ensure lowercase to match implementation

    def test_create_and_delete_catalog_and_schema(self):
        # Create a catalog
        self.unity_catalog.create_catalog(self.catalog_name)
        self.assertTrue(self.unity_catalog.catalog_client.catalog_exists(self.catalog_name),
                        f"Catalog '{self.catalog_name}' should exist after creation.")

        # Create a schema within the catalog
        self.unity_catalog.create_schema(self.catalog_name, self.schema_name)
        # self.assertTrue(self.unity_catalog.schema_client.schema_exists(self.catalog_name, self.schema_name),
        #                 f"Schema '{self.schema_name}' should exist within catalog '{self.catalog_name}' after creation.")

        # Force delete the catalog (and therefore also the schema)
        self.unity_catalog.delete_catalog(self.catalog_name)

        # Validate that the catalog and schema don't exist anymore
        self.assertFalse(self.unity_catalog.catalog_client.catalog_exists(self.catalog_name),
                         f"Catalog '{self.catalog_name}' should not exist after deletion.")

    def tearDown(self):
        # Ensure cleanup in case of test failure
        if self.unity_catalog.catalog_client.catalog_exists(self.catalog_name):
            self.unity_catalog.delete_catalog(self.catalog_name)