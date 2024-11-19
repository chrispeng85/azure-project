from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.graphrbac import GraphRbacManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.common.credentials import ServicePrincipalCredentials
import uuid 
import subprocess


def main():
    
    #creates a service principal with blob storage data contributor role which databricks can use to access the storage account


    subscription_id = "e7b6d171-1d67-440f-a8e4-2e1588380c2d"
    rg_name = "resource_group_oct31"
    storage_name = "storageaccountoct31"
    app_name = "databricks"


    credential = DefaultAzureCredential()

    tenant_id = "11302f44-4e24-4277-a212-f049836f4005"

    client_id = "2911e704-8a7f-4325-bf8d-d05cc89f49c3"
    


    resource_client = ResourceManagementClient(credential, subscription_id)
    auth_client = AuthorizationManagementClient(credential, subscription_id)
    graph_client = GraphRbacManagementClient(credential, tenant_id )
    storage_client = StorageManagementClient(credential, subscription_id)

    parameters = {
        "displayName": app_name,
        "identifierUris": [f"http://{app_name}-{uuid.uuid4()}"],
        "passwordCredentials": [{
            "startDate": "2024-01-01T00:00:00Z",
            "endDate": "2026-01-01T00:00:00Z",
            "value": str(uuid.uuid4()) #generate password
        }],
    }


    app = graph_client.applications.create(parameters) #create Azure AD application
    print(f"created azure AD application with ID: {app.app_id}")

    service_principal_params = {
        "appId": app.app_id,
        "accountEnabled": True
    }

    service_principal = graph_client.service_principals.create(service_principal_params)  #create service princial 
    print(f"created service principal with object id: {service_principal.object_id}")



    storage_account = storage_client.storage_accounts.get_properties(
        resource_group_name = rg_name, account_name = storage_name

    )

    role_assignment_params = {
        "principalId": service_principal.object_id,
        "roleDefinitionId": "/subscriptions/{subscription_id}/providers/Microsoft.Authorization/roleDefinitions/ba92f5b4-2d11-453d-a403-e96b0029c9fe",
        "principalType": "ServicePrincipal" 

    }

    auth_client.role_assignments.create( #assign role to storage account

        scope = storage_account.id,
        role_assignment_name = str(uuid.uuid4()),
        parameters = role_assignment_params

    )

    #databricks credentials
    #spark.conf.set("fs.azure.account.auth.type", "OAuth")
    #spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    #spark.conf.set("fs.azure.account.oauth2.client.id", app.app_id)
    #spark.conf.set("fs.azure.account.oauth2.client.secret", app_create_params['passwordCredentials'][0]['value'])
    #spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/11302f44-4e24-4277-a212-f049836f4005/oauth2/token")


    
if __name__  == "__main__":
    main()

    
