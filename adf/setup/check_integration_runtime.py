from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError



from azure.storage.blob import BlobServiceClient

import os


def verify_adf_access(subscription_id, rg_name, df_name):

    credential = DefaultAzureCredential()
    try:
        adf_client = DataFactoryManagementClient(credential, subscription_id)

        runtimes = list(adf_client.integration_runtimes.list_by_factory(
            resource_group_name = rg_name,
            factory_name = df_name  
        ))

        print("Available Integration Runtimes:")
        for runtime in runtimes:
            print(f"- {runtime.name} (Type: {runtime.type})")

        return True
    
    except Exception as e:
        print (f"Error accessing data factory: {str(e)}")


        return False
    

def main():

    subscription_id = "e7b6d171-1d67-440f-a8e4-2e1588380c2d"
    rg_name = "resource_group_oct31"
    df_name = "datafactoryoct31"

    verify_adf_access(subscription_id, rg_name, df_name)


if __name__ == "__main__":
    main()

    
