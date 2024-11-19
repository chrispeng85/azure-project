from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.keyvault.secrets import SecretClient



from azure.storage.blob import BlobServiceClient

import os

def create_source_dataset(subscription_id, rg_name, df_name, ds_name, ls_name, table_name):

   #creates a source dataset for the copy activity

    credential = DefaultAzureCredential()

    adf_client = DataFactoryManagementClient(credential, subscription_id)

    ls_reference = LinkedServiceReference(type = "LinkedServiceReference", reference_name = ls_name)

    ds_source = DatasetResource(properties = SqlServerTableDataset(type = "SqlServerTable", linked_service_name = ls_reference, table_name = table_name, schema = "SalesLT" ))

    ds = adf_client.datasets.create_or_update(rg_name, df_name, ds_name, ds_source)


def create_auto_resolve_ls(subscription_id, storage_account_name, rg_name, df_name, ls_name):

    #creates a linked service with auto resolve integration runtime and returns it
    
    credential = DefaultAzureCredential()
    adf_client = DataFactoryManagementClient(credential, subscription_id)
    storage_linked_service = LinkedServiceResource(
        #use AzureBlobFSLinkedService for azure data lake gen 2 storage
        #auto resolve integration runtime does not require the 'connect_via' parameter 
        properties = AzureBlobFSLinkedService(
            
            type = "AzureBlobFS",
            url = f"https://{storage_account_name}.dfs.core.windows.net"
        )

    )

    linked_service = adf_client.linked_services.create_or_update(

        resource_group_name = rg_name,
        factory_name = df_name,
        linked_service_name = ls_name,
        linked_service =  storage_linked_service
    )

    return linked_service

def create_container_in_storage(connection_string, container_name):

    #creates a bronze container in storage account

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    try: 
        container = blob_service_client.get_container_client(container_name)
        properties = container.get_container_properties()

        if properties.metadata.get('layer') == 'bronze':
            print(f"Bronze container already exists")

    except ResourceNotFoundError:

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        container_client = blob_service_client.create_container(name = container_name, metadata = {'layer': 'bronze'})

def create_sink_dataset(subscription_id, storage_account_name, ds_name, ls_name, rg_name, df_name, container_name):
    
    #creates a sink dataset that points towards azure datalake storage

    credential = DefaultAzureCredential()
    adf_client = DataFactoryManagementClient(credential, subscription_id)
    auto_resolve_ls = create_auto_resolve_ls(subscription_id, storage_account_name, rg_name, df_name, ls_name)
    folder_path = f"root/{container_name}"
    ls_reference = LinkedServiceReference(type = "LinkedServiceReference", reference_name = ls_name)
    ds_sink = DatasetResource(properties = ParquetDataset(type = "Parquet", linked_service_name = ls_reference,
          location = AzureBlobFSLocation(
            type = "AzureBlobFSLocation",
            file_system = container_name,
            folder_path = "root"

    )
      ))

    ds = adf_client.datasets.create_or_update(rg_name, df_name, ds_name, ds_sink)

def create_copy_activity(pipeline_name, dsin_name, dsout_name):

    #creates a copy data activity from sql server source to azure data lake storage sink in azure data factory
    


    dsin_ref = DatasetReference(reference_name = dsin_name, type = "DatasetReference")
    dsout_ref = DatasetReference(reference_name = dsout_name, type = "DatasetReference")
    copy_activity = CopyActivity(name = pipeline_name, inputs = [dsin_ref], outputs = [dsout_ref], 
                                 source = SqlServerSource(type = "SqlServerSource"), 
                                 sink = ParquetSink(type = "ParquetSink", store_settings = AzureBlobFSWriteSettings(type = "AzureBlobFSWriteSettings", copy_behavior = "PreserveHierarchy")) )


    return copy_activity

def run_copy_pipeline(copy_activity, adf_client, rg_name, df_name, p_name):

    #creates and runs the copy data pipeline using the copy_data activity created
    

    params_for_pipeline = {}

    p_obj = PipelineResource(
        activities = [copy_activity], parameters = params_for_pipeline)
    
    p = adf_client.pipelines.create_or_update(rg_name, df_name, p_name, p_obj)



    #create pipeline run
    run_response = adf_client.pipelines.create_run(rg_name, df_name, p_name, parameters = {})

    #moniter pipeline run

    pipeline_run = adf_client.pipeline_runs.get(
        rg_name, df_name, run_response.run_id
    )

    print("\n\tPipeline run status: {}".format(pipeline_run.status))


def main():

    subscription_id = "e7b6d171-1d67-440f-a8e4-2e1588380c2d"
    rg_name = "resource_group_oct31"
    df_name = "datafactoryoct31"
    storage_account_name = "storageaccountoct31"
    
    key_vault_url = "https://keyvaultoct31.vault.azure.net/"
    credential = DefaultAzureCredential()

    client = SecretClient(vault_url = key_vault_url, credential = credential)

    connection_string = client.get_secret("storageAccountAccessKey").value 

    
    

    adf_client = DataFactoryManagementClient(credential, subscription_id)

    auto_resolve_ls_name = "auto_resolve_ls"

    #auto_resolve_ls = create_auto_resolve_ls(adf_client, storage_account_name, rg_name, df_name, auto_resolve_ls_name, ir_name)

    source_ds_name = "source_ds"

    table_name = "SalesLT.address"


    ls_name = "linkedservice123"



    source_ds = create_source_dataset(subscription_id, rg_name, df_name, source_ds_name, ls_name, table_name)

    sink_ds_name = "sink_ds"

    container_name = "bronze"

    bronze_container = create_container_in_storage(connection_string, container_name)
    sink_ds = create_sink_dataset(subscription_id, storage_account_name, sink_ds_name, auto_resolve_ls_name, rg_name, df_name, container_name)

    activity_name = "copySqltoADLS"

    copy_activity = create_copy_activity(activity_name, source_ds_name, sink_ds_name)

    pipeline_name = "copy_pipeline"

    run_pipeline = run_copy_pipeline(copy_activity, adf_client, rg_name, df_name, pipeline_name)



if __name__ == "__main__":
    main()



    



    









