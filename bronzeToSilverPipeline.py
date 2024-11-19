from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
import textwrap
import logging 



def main():

    subscription_id = "e7b6d171-1d67-440f-a8e4-2e1588380c2d"
    rg_name = "resource_group_oct31"
    df_name = "datafactoryoct31"
    credential = DefaultAzureCredential()
    adf_client = DataFactoryManagementClient(credential, subscription_id)


    notebook_activity = DatabricksNotebookActivity(

        name = "bronze to silver",
        linked_service_name = LinkedServiceReference(
            reference_name = "AzureDatabricks1",
            type = "LinkedServiceReference"

        ),
        notebook_path =  "/Shared/bronze to silver",

        depends_on = [ActivityDependency(activity = "forEachTable",
                    dependency_conditions = ["Succeeded"])]
        
    )


    pipeline = adf_client.pipelines.get(
        rg_name,
        df_name,
        "lookup_foreach"
    )

    pipeline.activities.append(notebook_activity)

    adf_client.pipelines.create_or_update(
        rg_name,
        df_name,
        "lookup_foreach",
        pipeline = pipeline
    )


if __name__ == "__main__":
    main()