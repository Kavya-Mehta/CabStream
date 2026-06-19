# outputs.tf
# Exposes important values after terraform apply
# Use these to configure your pipeline and Databricks

output "resource_group_name" {
  description = "Resource group name"
  value       = azurerm_resource_group.cabstream.name
}

output "storage_account_name" {
  description = "ADLS Gen2 storage account name"
  value       = azurerm_storage_account.cabstream.name
}

output "storage_account_key" {
  description = "ADLS Gen2 storage account key"
  value       = azurerm_storage_account.cabstream.primary_access_key
  sensitive   = true
}

output "adls_endpoint" {
  description = "ADLS Gen2 endpoint for Databricks mount"
  value       = "abfss://delta@${azurerm_storage_account.cabstream.name}.dfs.core.windows.net"
}

output "eventhubs_connection_string" {
  description = "Event Hubs connection string for producer"
  value       = azurerm_eventhub_namespace_authorization_rule.cabstream.primary_connection_string
  sensitive   = true
}

output "eventhubs_namespace" {
  description = "Event Hubs namespace"
  value       = azurerm_eventhub_namespace.cabstream.name
}

output "databricks_workspace_url" {
  description = "Databricks workspace URL"
  value       = azurerm_databricks_workspace.cabstream.workspace_url
}

output "databricks_workspace_id" {
  description = "Databricks workspace ID"
  value       = azurerm_databricks_workspace.cabstream.workspace_id
}