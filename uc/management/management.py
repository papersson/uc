from uc.databricks.http_client import CatalogClient, SecurityGroupClient
from uc.utils.scim import StartsWith


class UnityCatalog:
    def __init__(self, access_token: str, databricks_instance: str):
        self.catalog_client = CatalogClient(access_token, databricks_instance)
        self.security_group_client = SecurityGroupClient(access_token, databricks_instance)

    def create_catalog_with_default_groups(self, catalog_name: str):
        normalized_name = catalog_name.lower()
        self.catalog_client.create_catalog(normalized_name)
        
        # Define groups and their privileges
        groups_privileges = {
            f"{normalized_name}_read": ["USE CATALOG", "USE SCHEMA", "SELECT"],
            f"{normalized_name}_readwrite": ["ALL PRIVILEGES"],
            f"{normalized_name}_write_metadata": ["APPLY TAG"],
        }

        # Create groups and assign privileges
        for group, privileges in groups_privileges.items():
            self.security_group_client.create_security_group(group)
            # Assume a method to assign privileges to a group directly, possibly extending existing functionalities
            self.security_group_client.assign_privileges_to_group("CATALOG", normalized_name, group, privileges)

    def delete_catalog_and_groups(self, catalog_name: str):
        normalized_name = catalog_name.lower()
        self.catalog_client.delete_catalog(normalized_name)

        # Fetch and delete groups associated with this catalog
        groups = self.security_group_client.fetch_groups([StartsWith('displayName', normalized_name)])
        for group in groups:
            if normalized_name in group["displayName"].lower():
                self.security_group_client.delete_security_group(group["id"])

    # The method to assign people or groups to privileges would depend on the implementation details of how people and groups are managed and referenced in your system
