from azure.identity import DefaultAzureCredential
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import Factory, FactoryIdentity
import uuid

def assign_contributor_role(subscription_id, rg_name, df_name):

    
    #this method assigns a contributor role to the data factory's managed identity
    
    try:
        credential = DefaultAzureCredential()

        auth_client = AuthorizationManagementClient(credential, subscription_id)
        adf_client = DataFactoryManagementClient(credential, subscription_id)

        factory = adf_client.factories.get(rg_name, df_name)

        if factory.identity is None:
            
            print("data factory managed identity is not enabled")

            factory_identity = FactoryIdentity(type = 'SystemAssigned')
            factory_params = Factory(location = factory.location, identity = factory_identity)

            factory = adf_client.factories.begin_create_or_update(
                resource_group_name = rg_name,
                factory_name = df_name,
                factory = factory_params
            ).result()



            


        principal_id = factory.identity.principal_id

        scope = f"/subscriptions/{subscription_id}/resourceGroups/{rg_name}"

        contributor_role_id = "b24988ac-6180-42a0-ab88-20f7382dd24c"

        #uuid role assignment name to fulfill azure naming standards
        role_assignment_name = str(uuid.uuid4())

        assignment_params = {
            "role_definition_id": f"/subscriptions/{subscription_id}/providers/Microsoft.Authorization/roleDefinitions/{contributor_role_id}",
            "principal_id": principal_id,
            "principal_type": "ServicePrincipal"
        }


        auth_client.role_assignments.create(

            scope = scope,
            role_assignment_name = role_assignment_name,
            parameters = assignment_params 
        )

        print(f"assigned contributor role to data factory")

    except Exception as e:
        print(f"an error occurred: {str(e)}")



def main():


    subscription_id = "e7b6d171-1d67-440f-a8e4-2e1588380c2d"
    rg_name = "resource_group_oct31"
    df_name = "datafactoryoct31"

    assign_contributor_role(subscription_id, rg_name, df_name)



if __name__ == "__main__":
    main()

    
    
