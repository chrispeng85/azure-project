from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from azure.storage.blob import BlobServiceClient
from datetime import datetime 
from azure.keyvault.secrets import SecretClient

#creates a linked service to azure databricks using databricks token as credential


def main():
    subscription_id = "e7b6d171-1d67-440f-a8e4-2e1588380c2d"
    resource_group_name = "resource_group_oct31"
    data_factory_name = "datafactoryoct31"

    credential = DefaultAzureCredential()

    key_vault_url = "https://keyvaultoct31.vault.azure.net/"
    client = SecretClient(vault_url = key_vault_url, credential = credential)

    access_token = client.get_secret("databricksToken").value 

    resource_client = ResourceManagementClient(credential, subscription_id)
    adf_client = DataFactoryManagementClient(credential, subscription_id)

    ls_name = "databricksls"

    properties = AzureDatabricksLinkedService(

        domain = "https://adb-1296812669264659.19.azuredatabricks.net/browse/folders/2574222890598090?o=1296812669264659", 
        access_token = access_token,
        existing_cluster_id = "1113-161629-roo87fmm"
    )

    linked_service = LinkedServiceResource(properties = properties)

    adf_client.linked_services.create_or_update(
        resource_group_name = resource_group_name,
        factory_name = data_factory_name,
        linked_service_name = ls_name,
        linked_service = linked_service
    )


if __name__ == "__main__":
    main()