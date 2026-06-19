# storage.tf
# ADLS Gen2 storage account for Delta tables
# 
# Why ADLS Gen2 over regular Blob Storage:
# - Hierarchical namespace enables folder-level operations
# - Delta Lake requires atomic directory operations
# - 10x better performance for big data workloads
# - Databricks reads natively, no extra connectors needed
#
# Why LRS (Locally Redundant Storage):
# - 3 copies within same datacenter
# - Cheapest option (~$0.02/GB)
# - Fine for a portfolio project
# - Production would use ZRS or GRS for disaster recovery

resource "azurerm_storage_account" "cabstream" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.cabstream.name
  location                 = azurerm_resource_group.cabstream.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"

  # This enables ADLS Gen2 (hierarchical namespace)
  # Must be enabled at creation time, cannot be changed later
  is_hns_enabled = true

  tags = {
    project     = var.project_name
    owner       = var.owner_email
    environment = "dev"
    managed_by  = "terraform"
  }
}

# Container for Delta tables
# Bronze, Silver, Gold all land here
resource "azurerm_storage_container" "delta" {
  name                  = "delta"
  storage_account_name  = azurerm_storage_account.cabstream.name
  container_access_type = "private"
}

# Container for raw data files (Parquet)
resource "azurerm_storage_container" "raw" {
  name                  = "raw"
  storage_account_name  = azurerm_storage_account.cabstream.name
  container_access_type = "private"
}