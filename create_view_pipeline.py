from azure.synapse.artifacts.models import *
from azure.synapse.artifacts import ArtifactsClient
from azure.identity import DefaultAzureCredential

def main():

    subscription_id = "e7b6d171-1d67-440f-a8e4-2e1588380c2d"
    workspace_name = "synapse-workspace-oct31"
    rg_name = "resource_group_oct31"
    workspace_name = "synapse-workspace-oct31"
    storage_account_name = "storageaccountoct31"


    credential = DefaultAzureCredential()
    artifacts_client = ArtifactsClient(
        credential = credential,
        endpoint = "https://synapse-workspace-oct31.dev.azuresynapse.net"

    )

    pipeline = {
        "activities": [
            {

                "name": "Get Tablenames",
                "type": "GetMetadata",
                "typeProperties": {
                    "fieldList": ["childItems"],
                    "dataset": {
                        "referenceName": "goldtable",
                        "type": "DatasetReference"
                    }
                }
            },
            {
                "name": "ForEachTableName",
                "type": "ForEach",
                "typeProperties": {
                    "items": {

                        "value": "@activity('Get Tablenames').output.childItems",
                        "type": "Expression"

                    },
                    "activities": [
                        {
                        "name": "StoredProcedure",
                        "type": "SqlServerStoredProcedure",
                        "linkedServiceName": {
                            "referenceName": "synapse-workspace-oct31-WorkspaceDefaultStorage",
                            "type": "LinkedServiceReference"

                        },
                        "typeProperties": {
                            "storedProcedureName": "[dbo].[CreateSQLServerlessView_gold]",
                            "storedProcedureParameters": {
                                "ViewName": {
                                    "value": "@item().name",
                                    "type": "Expression"
                                }

                            }
                        }
                        }
                    ],
                    "isSequential": True

                }



            }

        ]

    }

    response = artifacts_client.create_or_update_pipeline_initial(
        pipeline_name = "create view",
        pipeline = pipeline

    )

    if __name__ == "__main__":
        main()