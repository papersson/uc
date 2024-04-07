import os
from uc.databricks.http_client import CatalogClient, SecurityGroupClient
from uc.utils.scim import StartsWith


class UnityCatalog:
    def __init__(self):
        self.catalog_client = CatalogClient()
        self.security_group_client = SecurityGroupClient()

    def create_catalog(self, catalog_name: str):
        # Create catalog
        catalog_name = catalog_name.lower()
        self.catalog_client.create_catalog(catalog_name)
        
        # Create groups and assign privileges
        groups_privileges = {
            f"{catalog_name}_read": ["USE CATALOG", "USE SCHEMA", "SELECT"],
            f"{catalog_name}_readwrite": ["ALL PRIVILEGES"],
            f"{catalog_name}_write_metadata": ["APPLY TAG"],
        }
        for group, privileges in groups_privileges.items():
            self.security_group_client.create_security_group(group)
            self.security_group_client.assign_privileges_to_group("CATALOG", catalog_name, group, privileges)

    def delete_catalog(self, catalog_name: str):
        # Delete catalog
        catalog_name = catalog_name.lower()
        self.catalog_client.delete_catalog(catalog_name)

        # Delete groups associated with catalog
        groups = self.security_group_client.fetch_groups([StartsWith('displayName', catalog_name)])
        for group in groups:
            if catalog_name in group["displayName"].lower():
                self.security_group_client.delete_security_group(group["id"])

    # The method to assign people or groups to privileges would depend on the implementation details of how people and groups are managed and referenced in your system
