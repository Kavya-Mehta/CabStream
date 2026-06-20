# keyvault.tf
# Azure Key Vault for secret management
#
# Why Key Vault:
# - Secrets never in code or config files
# - Databricks reads directly from Key Vault
# - Audit log: who accessed which secret when
# - Rotate secrets without changing code
# - Production standard at every major company
# - At Google/Meta/Uber, hardcoded credentials
#   would fail code review immediately

data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "cabstream" {
  name                = "cabstream-kv"
  location            = azurerm_resource_group.cabstream.location
  resource_group_name = azurerm_resource_group.cabstream.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  soft_delete_retention_days = 7
  purge_protection_enabled   = false

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    secret_permissions = [
      "Get",
      "List",
      "Set",
      "Delete",
      "Purge"
    ]
  }

  tags = {
    project     = var.project_name
    owner       = var.owner_email
    environment = "dev"
    managed_by  = "terraform"
  }
}

resource "azurerm_key_vault_secret" "storage_key" {
  name         = "adls-storage-key"
  value        = azurerm_storage_account.cabstream.primary_access_key
  key_vault_id = azurerm_key_vault.cabstream.id
}

resource "azurerm_key_vault_secret" "eventhubs_connection" {
  name         = "eventhubs-connection-string"
  value        = azurerm_eventhub_namespace_authorization_rule.cabstream.primary_connection_string
  key_vault_id = azurerm_key_vault.cabstream.id
}