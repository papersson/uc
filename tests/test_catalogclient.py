import os  # Add this import
from dotenv import load_dotenv  # Add this import
from unittest import TestCase
from uc.databricks.http_client import CatalogClient

load_dotenv()  # This loads the variables from .env into the environment

class TestCatalogClientIntegration(TestCase):
    def setUp(self):
        # Use environment variables for token and instance URL
        access_token = os.getenv('DATABRICKS_ACCESS_TOKEN')
        databricks_instance = os.getenv('DATABRICKS_INSTANCE_URL')
        self.client = CatalogClient(access_token, databricks_instance)

    def test_create_and_delete_catalog_integration(self):
        # Create a test catalog
        create_response = self.client.create_catalog("integration_test_catalog")
        self.assertIn(create_response.status_code, [200, 201])
        
        # Clean up: delete the test catalog
        delete_response = self.client.delete_catalog("integration_test_catalog", force=True)
        self.assertIn(delete_response.status_code, [200, 204])
