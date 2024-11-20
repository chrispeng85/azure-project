from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
import textwrap
import logging 

logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)

#creates a lookup activity, which retrieves the schema & table name of all files, and a for each activity with copy activity nested 
#within, to copy all files using the output of the lookup activity


def verify_linked_service(subscription_id, rg_name, df_name, ls_name):

    #verifies the linked service exists
    credential = DefaultAzureCredential()
    adf_client = DataFactoryManagementClient(credential, subscription_id)
    try:
        adf_client.linked_services.get(
                rg_name,
                df_name,
                ls_name 
        )
        print("successfully found linked service")
        return True
    
    except Exception as e:
        logger.error(f"linked service validation failed")
        return False 



def create_lookup_pipeline( activity_name, source_query, source_ds_name):

    #creates a lookup activity using an sql server source dataset and custom query

    source_query = textwrap.dedent(source_query).strip()
    lookup_activity = {

        "name": activity_name,
        "type": "Lookup",
        "dependsOn": [],
        "typeProperties": {
            "source": {
                "type": "SqlServerSource",
                "sqlReaderQuery": source_query,
                "queryTimeout": "02:00:00"
            },
            "dataset": {
                "referenceName": source_ds_name,
                "type": "DatasetReference"
            },
            "firstRowOnly": False
        }
    }

    return lookup_activity

def create_copy_source(rg_name, df_name,ds_name, subscription_id, ls_name):
    
    #creates the source dataset for copy activity
    credential = DefaultAzureCredential()
    adf_client = DataFactoryManagementClient(credential, subscription_id)
    ls_reference = LinkedServiceReference(type = "LinkedServiceReference", reference_name = ls_name )

    ds_source = {
        "properties": {
            "type": "SqlServerTable",
            "linkedServiceName": {
                "referenceName": ls_name,
                "type": "LinkedServiceReference"
            }
        },
        "typeProperties": {
            "query": {
                "value": "@concat('SELECT * FROM ', item().SchemaName, '.', item().TableName))",
                "type": "Expression"

            }

        }
    }


    ds = adf_client.datasets.create_or_update(rg_name, df_name, ds_name, ds_source)
    return ds


def create_copy_sink(rg_name, df_name, ds_name, subscription_id, ls_name, lookup_activity_name):

     #creates a sink dataset for the copy activity
    credential = DefaultAzureCredential()
    adf_client = DataFactoryManagementClient(credential, subscription_id)
    ls_reference = LinkedServiceReference(type = "LinkedServiceReference", reference_name = ls_name)
    ds_sink = {
        "properties": {
            "type": "Parquet",
            "linkedServiceName": {
                "referenceName": ls_name,
                "type": "LinkedServiceReference"
            },
            "parameters": {

                "schemaName": {
                "type": "String",
                "defaultValue": {
                     "value": "@item().SchemaName",
                    "type": "Expression"
                        }
            },
                "tableName": {
                "type": "String",
                "defaultValue": {
                    "value": "@item().TableName",
                    "type": "Expression"
                }
            }
            },

            "typeProperties": {
                "location": {
                    "type": "AzureBlobFSLocation",
                    "fileSystem": "bronze",
                    "folderPath": {
                        "value": "@concat('root','/',dataset().schemaName, '/', dataset().tableName)",
                        "type": "Expression"
                    },
                    "fileName": {
                        "value": "@concat(dataset().tableName, '.parquet')",
                        "type": "Expression"
                    }

                }
            }
        }
    }




    ds = adf_client.datasets.create_or_update(rg_name, df_name, ds_name, ds_sink )
    return ds


def create_copy_activity(source_ds_name, sink_ds_name):

    #creates the copy activity using the source and sink datasets previously created
    copy_activity_name = "forEachCopy"

    dsin_reference = DatasetReference(type = "datasetReference", reference_name = source_ds_name )
    dsout_reference = DatasetReference(type = "datasetReference", reference_name = sink_ds_name)

    copy_activity = {

        "name": "forEachCopy",
        "type": "Copy",
        "dependsOn": [],
        "policy": {
            "timeout": "7.00:00:00",
            "retry": 0,
            "retryIntervalInSeconds": 30,
            "secureInput": False,
            "secureOutput": False
        },
        "typeProperties": {
            "source" : {
                "type": "SqlServerSource",
                "sqlReaderQuery": {

                    "value": "SELECT * FROM [@{item().SchemaName}].[@{item().TableName}]",
                    "type": "Expression"
                },
                "queryTimeout": "02:00:00"
            },

            "sink": {
                "type": "ParquetSink",
                "storeSettings": {
                    "type": "AzureBlobFSWriteSettings",
                    "copyBehavior": "PreserveHierarchy"
                }
            }
        },
        "inputs": [
            {

                "referenceName": source_ds_name,
                "type": "DatasetReference"
            }
        ],
        "outputs": [
            {   
                "referenceName": sink_ds_name,
                "type": "DatasetReference"

            }
        ]
    }

    return copy_activity


def create_foreach_activity(copy_activity,lookup_activity_name):

    #creates a for each activity with a copy activity nested within, depending on the lookup activity previously created
    
    dynamic_expression = f"@activity('{lookup_activity_name}').output.value"
    foreach_activity = {

        "name": "forEachTable",
        "type": "ForEach",
        "dependsOn": [
            {
                "activity": lookup_activity_name,
                "dependencyConditions": ["Succeeded"]
            }
        ],
        "typeProperties": {

            "items":
             {
              "value": "@activity('" + lookup_activity_name + "').output.value",
              "type": "Expression"
             },

            "activities": [copy_activity],
            "isSequential": True

        }
    }

    return foreach_activity

def main():

    rg_name = "resource_group_oct31"
    df_name = "datafactoryoct31"
    subscription_id = "e7b6d171-1d67-440f-a8e4-2e1588380c2d"

    #query used in lookup activity
    source_query = """

    SELECT 
        s.name AS SchemaName,
        t.name AS TableName
    FROM 
        sys.tables t 
    INNER JOIN 
        sys.schemas s 
    ON 
        t.schema_id = s.schema_id 
    WHERE 
        s.name = 'SalesLT'

    """
    #dynamic query used in copy_each activity source dataset
    
    foreach_source_query =  """
    @{
        concat('SELECT * FROM', item().SchemaName, '.', item().TableName )
    }

    """

    credential = DefaultAzureCredential()
    adf_client = DataFactoryManagementClient(credential, subscription_id)

    #create linked service reference
    ls_name = "linkedservice123"
    ls_reference = LinkedServiceReference(
        type = "linkedServiceReference", 
        reference_name = ls_name)
    
    sink_ls_name = "auto_resolve_ls"
    sink_ls_reference = LinkedServiceReference(
        type = "linkedServiceReference",
        reference_name = sink_ls_name
    )
    

    #create source dataset for lookup activity
    source_ds = DatasetResource(
        properties = SqlServerTableDataset(
            type = "SqlServerTable",
            linked_service_name =  ls_reference,
            schema = "sys",
            table = "tables"

    )
    )

    #create lookup activity
    lookup_source_name = "sourcedataset"
    adf_client.datasets.create_or_update(rg_name, df_name, lookup_source_name, source_ds)
    verify_linked_service(subscription_id = subscription_id, rg_name = rg_name, df_name = df_name, ls_name = ls_name )
    lookup_activity_name = "lookupAllTables"
    lookup_activity = create_lookup_pipeline(lookup_activity_name, source_query, lookup_source_name)


    #create source & sink datasets and copy activity
    source_ds_name = "copy_source_ds"
    sink_ds_name = "copy_sink_ds"
    create_copy_source(rg_name, df_name, source_ds_name, subscription_id, ls_name)
    create_copy_sink(rg_name, df_name, sink_ds_name, subscription_id, sink_ls_name, lookup_activity_name)
    copy_activity = create_copy_activity(source_ds_name = source_ds_name, sink_ds_name = sink_ds_name)

    #create foreach activity using copy activity
    foreach_activity = create_foreach_activity(copy_activity, lookup_activity_name)

    #create pipeline
    pipeline_name = "lookup_foreach"

    lookup_foreach_pipeline = {
        "properties": {
            "activities" : [lookup_activity, foreach_activity],
            "variables" : {
                "ProcessingDate": {
                        "type": "String",
                        "defaultValue": "@utcnow()"
                    }
                }

            }    
        }
    
    adf_client.pipelines.create_or_update(rg_name, df_name, pipeline_name, pipeline = lookup_foreach_pipeline)

    run = adf_client.pipelines.create_run(rg_name, df_name, pipeline_name, parameters = {} )
    run_response = adf_client.pipeline_runs.get(rg_name, df_name, run.run_id)

    print("\n\tPipeline run status: {}".format(run_response.status))


if __name__ == "__main__":
    main()




