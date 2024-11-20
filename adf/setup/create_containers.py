from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from azure.storage.blob import BlobServiceClient
from datetime import datetime 

from azure.keyvault.secrets import SecretClient

#creates bronze, silver and gold containers within the storage account

def main():
    

    key_vault_url = "https://keyvaultoct31.vault.azure.net/"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url = key_vault_url, credential = credential)

    connection_string = client.get_secret("storageAccountAccessKey").value


    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    containers = ['bronze', 'silver', 'gold']

    for container in containers:
        container_client = blob_service_client.create_container(
            name = container,
            metadata = {'created': datetime.utcnow().isoformat(),
                        'purpose': f'{container} layer storage'}
        )

if __name__ == "__main__":
    main()

    


