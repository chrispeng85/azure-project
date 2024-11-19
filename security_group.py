from azure.identity import DefaultAzureCredential
from azure.graphrbac import GraphRbacManagementClient
from azure.mgmt.authorization import AuthorizationManagementClient
import requests 


def main():

    credential = DefaultAzureCredential()
    token = credential.get_token('https://graph.microsoft.com/.default')

    headers = {
        'Authorization': f'Bearer {token.token}',
        'Content-Type': 'application/json'
    }

    graph_url = 'https://graph.microsoft.com/v1.0'

    user_id = "9c80ce32-16d5-49e6-8787-0a5daec11718"
    group_name = "azure security group"

    
    group_data = {
        "displayName": group_name,
        "description": "security group for managing azure project access",
        "securityEnabled": True,
        "mailEnabled": False,
        "mailNickname": "securitygroup"

    }

    response = requests.post(
        f'{graph_url}/groups',
        headers = headers,
        json = group_data
    )

    response.raise_for_status()

    group_id = response.json()['id']

    #print(f"Security group '{group_name}' created successfully")


    #add user to group
    member_data = {

        "@odata.id": f"{graph_url}/directoryObjects/{user_id}"
    }

    response = requests.post(
        f'{graph_url}/groups/{group_id}/members/$ref',
        headers = headers,
        json = member_data
    )

    response.raise_for_status()


if __name__ == "__main__":
    main()