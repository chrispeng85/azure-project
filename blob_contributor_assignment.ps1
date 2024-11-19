
$storageAccountName = "storageaccountoct31"
$container = "bronze"
$identityId = "e5588baa-b609-416d-851e-9bd79349544f"
$rgName = "resource_group_oct31"
$subscriptionId = "e7b6d171-1d67-440f-a8e4-2e1588380c2d"
$dfName = "datafactoryoct31"

$dataFactory = Get-AzDataFactoryV2 -ResourceGroupName $rgName
$managedIdentityId = $dataFactory.Identity.PrincipalId 


$storageAccount = Get-AzStorageAccount -ResourceGroupName $rgName
$storageAccountId = $storageAccount.Id 

$roleDefinitionId = (Get-AzRoleDefinition -Name "Storage Blob Data Contributor").id 


New-AzRoleAssignment -ObjectId $managedIdentityId -RoleDefinitionName "Storage Blob Data Contributor" -Scope $storageAccountId
