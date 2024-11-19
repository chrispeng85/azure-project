terraform {


  required_providers {

    azurerm = {

      source  = "hashicorp/azurerm"
      version = ">=3.0.0"

    }

    databricks = {

      source  = "databricks/databricks"
      version = "~>1.33.0"
    }

  }
}

provider "azurerm" {

  features {

    key_vault {

      purge_soft_delete_on_destroy    = true
      recover_soft_deleted_key_vaults = true


    }




  }

  subscription_id = "e7b6d171-1d67-440f-a8e4-2e1588380c2d"

}


resource "azurerm_resource_group" "azure_rg_oct31" {

  name = "resource_group_oct31"

  location = "West Europe"

  tags = {

    environment = "dev"

    source = "Terraform"

    owner = "Dingkang Peng"

  }

}


resource "azurerm_data_factory" "azure_df_oct31" {

  name = "datafactoryoct31"

  location = azurerm_resource_group.azure_rg_oct31.location

  resource_group_name = azurerm_resource_group.azure_rg_oct31.name


}


resource "azurerm_databricks_workspace" "azure_db_oct31" {

  name                = "databricks_oct31"
  location            = azurerm_resource_group.azure_rg_oct31.location
  resource_group_name = azurerm_resource_group.azure_rg_oct31.name
  sku                 = "standard"


  tags = {

    environment = "dev"
  }


}


resource "azurerm_storage_account" "azure_sa_oct31" {

  name = "storageaccountoct31"

  resource_group_name      = azurerm_resource_group.azure_rg_oct31.name
  location                 = azurerm_resource_group.azure_rg_oct31.location
  account_tier             = "Standard"
  account_replication_type = "GRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = "true"



  tags = {


    environment = "staging"
  }

}


resource "azurerm_key_vault" "azure_kv_oct31" {

  name                        = "keyvaultoct31"
  location                    = azurerm_resource_group.azure_rg_oct31.location
  resource_group_name         = azurerm_resource_group.azure_rg_oct31.name
  enabled_for_disk_encryption = true
  tenant_id                   = "11302f44-4e24-4277-a212-f049836f4005"
  soft_delete_retention_days  = 7
  purge_protection_enabled    = false

  sku_name = "standard"

}


resource "azurerm_storage_data_lake_gen2_filesystem" "azure_fs_oct31" {

  name               = "datalake-fs-oct31"
  storage_account_id = azurerm_storage_account.azure_sa_oct31.id

  properties = {


  }




}




resource "azurerm_synapse_workspace" "azure_sw_oct31" {

  name                                 = "synapse-workspace-oct31"
  resource_group_name                  = azurerm_resource_group.azure_rg_oct31.name
  location                             = azurerm_resource_group.azure_rg_oct31.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.azure_fs_oct31.id
  sql_administrator_login              = "AdminUser"
  sql_administrator_login_password     = "Password2024!"

  identity {

    type = "SystemAssigned"

  }

  tags = {

    environment = "dev"


  }

}

provider "databricks" {
  host                        = azurerm_databricks_workspace.azure_db_oct31.workspace_url
  azure_workspace_resource_id = azurerm_databricks_workspace.azure_db_oct31.id

}

resource "databricks_cluster" "single_node" {

  cluster_name  = "single-node-minimal"
  spark_version = "13.3.x-scala2.12"
  node_type_id  = "Standard_DS3_v2"

  spark_conf = {
    "spark.databricks.cluster.profile" : "singleNode"
    "spark.master" : "local[*]"
    "spark.databricks.passthrough.enabled" : "true"
    "spark.databricks.acl.dfAclsEnabled" : "true"
    "spark.databricks.azure.workspace.id" : "${azurerm_databricks_workspace.azure_db_oct31.workspace_id}"
    "spark.databricks.delta.preview.enabled" : "true"

  }

  azure_attributes {
    availability       = "ON_DEMAND_AZURE"
    first_on_demand    = 1
    spot_bid_max_price = -1
  }

  autotermination_minutes = 15
  custom_tags = {
    "ResourceClass" = "SingleNode"

  }

  single_user_name = var.single_user_name

}

variable "location" {

  description = "Azure region for resources"
  type        = string
  default     = "West Europe"
}

variable "single_user_name" {
  description = "active directory principal id"
  type        = string
  default     = "d7peng_gmail.com#EXT#@d7penggmail.onmicrosoft.com"
}

output "workspace_url" {

  value = azurerm_databricks_workspace.azure_db_oct31.workspace_url

}

output "cluster_id" {

  value = databricks_cluster.single_node.id

}

output "workspace_id" {

  value = azurerm_databricks_workspace.azure_db_oct31.workspace_id
}


