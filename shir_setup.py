from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *

from azure.identity import DefaultAzureCredential

def create_shir(subscription_id, resource_group, factory_name, runtime_name):

    #this method creates a self-hosted integration runtime.
    #arguments: 
    #subscription_id: azure subsription id;
    #resource_group: name of the azure resource group being used;
    #factory_name: name of the azure data factory being used;
    #runtime_name: the name for the soon-to-be-created integration runtime
    
    credential = DefaultAzureCredential()

    adf_client = DataFactoryManagementClient(credential, subscription_id)

   

    create_runtime = adf_client.integration_runtimes.create_or_update(

        resource_group_name = resource_group,
        factory_name = factory_name,
        integration_runtime_name = runtime_name,
        integration_runtime = {"properties": {"description": "self hosted integration runtime", "type": "SelfHosted"}}
    )

    auth_keys = adf_client.integration_runtimes.list_auth_keys(
            resource_group_name = resource_group,
            factory_name = factory_name,
            integration_runtime_name = runtime_name

    )

    print("auth key 1: " + auth_keys.auth_key1)
    print("auth key 2: " + auth_keys.auth_key2)


def main():

    subscription_id ="e7b6d171-1d67-440f-a8e4-2e1588380c2d"
    resource_group_name = "resource_group_oct31"
    factory_name = "datafactoryoct31"
    runtime_name = "newruntime"

    create_shir(subscription_id, resource_group_name, factory_name, runtime_name)


if __name__ == "__main__":
    main()

    


    
#key 1:
#IR@4e53f664-8060-4f21-97fe-24ffee029fa3@datafactoryoct31@ServiceEndpoint=datafactoryoct31.westeurope.datafactory.azure.net@W1JUkjd+fQjZSXGMu/uS0oR0GDN8+r0B2wm03V8+Yc8=

#key 2:
#IR@4e53f664-8060-4f21-97fe-24ffee029fa3@datafactoryoct31@ServiceEndpoint=datafactoryoct31.westeurope.datafactory.azure.net@YM7KZQBqb/TWnMtGJIPoPSqtYKr+TELhyEWe3u6qJoI=