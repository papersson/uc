import os
from unittest import TestCase
from uc.databricks.http_client import CatalogClient, SecurityGroupClient
from uc.management import UnityCatalog
from uc.utils.scim import StartsWith

class TestUnityCatalogIntegration(TestCase):
    def setUp(self):
        access_token = os.getenv('DATABRICKS_ACCESS_TOKEN')
        databricks_instance = os.getenv('DATABRICKS_INSTANCE_URL')
        self.unity_catalog = UnityCatalog(access_token, databricks_instance)
        self.catalog_client = CatalogClient(access_token, databricks_instance)
        self.security_group_client = SecurityGroupClient(access_token, databricks_instance)

    def test_create_and_delete_catalog_with_groups_integration(self):
        catalog_name = "integration_test_catalog"
        
        # Create catalog and groups
        self.unity_catalog.create_catalog_with_default_groups(catalog_name)
        self.assertTrue(self.catalog_client.catalog_exists(catalog_name))
        groups = self.security_group_client.fetch_groups([StartsWith('displayName', catalog_name)])
        self.assertEqual(len(groups), 3)
        
        # Clean up: delete the catalog and groups
        self.unity_catalog.delete_catalog_and_groups(catalog_name)
        self.assertFalse(self.catalog_client.catalog_exists(catalog_name))
        groups = self.security_group_client.fetch_groups([StartsWith('displayName', catalog_name)])
        self.assertEqual(len(groups), 0)