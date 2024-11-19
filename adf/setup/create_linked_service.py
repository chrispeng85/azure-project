from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from azure.keyvault.secrets import SecretClient



def get_key_vault_secret(key_vault_url, secret_name):

    #retrieves secrets from azure key vault which will be used to retrieve the SQL login credentials.
    credential = DefaultAzureCredential()
    secret_client = SecretClient(vault_url = key_vault_url, credential = credential)

    return secret_client.get_secret(secret_name).value



def main():

    subscription_id = "e7b6d171-1d67-440f-a8e4-2e1588380c2d"
    resource_group_name = "resource_group_oct31"
    data_factory_name = "datafactoryoct31"
    server_name = "DESKTOP-T96Q0DK\SQLEXPRESS"
    database_name = "AdventureWorksLT2022"
    KVUri = "https://keyvaultoct31.vault.azure.net"
    secret_username = "username"
    secret_password = "password"
    integration_runtime_name = "newruntime"


    credentials = DefaultAzureCredential()

    resource_client = ResourceManagementClient(credentials, subscription_id)
    adf_client = DataFactoryManagementClient(credentials, subscription_id)

    ls_name = "linkedservice123"


    username = get_key_vault_secret(KVUri, secret_username)
    password = get_key_vault_secret(KVUri, secret_password)


    ir_reference = IntegrationRuntimeReference(reference_name = integration_runtime_name, type = "IntegrationRuntimeReference")



    connection_string = (f"Server = {server_name};"
                         f"Database = {database_name};"
                         f"User ID = {username};"
                         f"Password = {password};"
                         f"Encrypt = True;"
                         f"TrustServerCertificate = True;")
    

    sql_linked_service = LinkedServiceResource(
            properties = SqlServerLinkedService(
                   connection_string = connection_string,
                   connect_via = ir_reference
            )
    )

    response = adf_client.linked_services.create_or_update(

        resource_group_name = resource_group_name,
        factory_name = data_factory_name,
        linked_service_name = ls_name,
        linked_service = sql_linked_service
    )

    

if __name__ == "__main__":
    main()
