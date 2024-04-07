import os  # Add this import
from dotenv import load_dotenv  # Add this import
from unittest import TestCase
from uc.databricks.http_client import CatalogClient, DatabricksHttpService, SecurityGroupClient

load_dotenv()  # This loads the variables from .env into the environment

class TestCatalogClientIntegration(TestCase):
    def setUp(self):
        databricks_http_service = DatabricksHttpService()
        self.client = CatalogClient(databricks_http_service)

    def test_create_and_delete_catalog_integration(self):
        # Create a test catalog
        create_response = self.client.create_catalog("integration_test_catalog")
        self.assertIn(create_response.status_code, [200, 201])
        
        # Clean up: delete the test catalog
        delete_response = self.client.delete_catalog("integration_test_catalog", force=True)
        self.assertIn(delete_response.status_code, [200, 204])

class TestSecurityGroupClientIntegration(TestCase):
    def setUp(self):
        databricks_http_service = DatabricksHttpService()
        self.client = SecurityGroupClient(databricks_http_service)

    def test_create_and_delete_security_group_integration(self):
        # Create a test security group
        create_response = self.client.create_security_group("integration_test_group")
        self.assertIn(create_response.status_code, [200, 201])
        
        group_id = create_response.json().get('id')
        
        # Clean up: delete the test security group
        delete_response = self.client.delete_security_group(group_id)
        self.assertIn(delete_response.status_code, [200, 204])