
#user principal name: d7peng_gmail.com#EXT#@d7penggmail.onmicrosoft.com

#user object id: 9c80ce32-16d5-49e6-8787-0a5daec11718

#subscription id: e7b6d171-1d67-440f-a8e4-2e1588380c2d

#resource group name: resource_group_oct31

#key vault name: keyvaultoct31


#assign key vault administrator role to account
New-AzRoleAssignment -ObjectId "9c80ce32-16d5-49e6-8787-0a5daec11718" -RoleDefinitionName "Key Vault Administrator" -Scope "/subscriptions/e7b6d171-1d67-440f-a8e4-2e1588380c2d/resourceGroups/resource_group_oct31/providers/Microsoft.KeyVault/vaults/keyvaultoct31"

#give account permission to access secrets and keys in key vault
Set-AzKeyVaultAccessPolicy -VaultName "keyvaultoct31" -ObjectId "9c80ce32-16d5-49e6-8787-0a5daec11718" -PermissionsToSecrets all -PermissionsToKeys all