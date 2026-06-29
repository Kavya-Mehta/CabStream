# databricks.tf
# Azure Databricks workspace
#
# Why Databricks over raw Spark on VMs:
# - Managed Spark: no cluster setup, no JVM tuning
# - Built-in Delta Lake support
# - Reads Azure Open Dataset natively (1B+ rows, zero download)
# - Auto-scaling: adds nodes when processing 1B rows, removes when done
# - Notebook interface for exploration
# - Same PySpark code from local dev runs unchanged
# - Unity Catalog for data governance (production feature)
#
# Why Premium tier:
# - Required for Key Vault-backed secret scopes (used in databricks_cluster.tf)
# - Adds RBAC and Unity Catalog support
# - ~40% more DBU cost than Standard, justified by secret scope requirement

resource "azurerm_databricks_workspace" "cabstream" {
  name                = var.databricks_workspace_name
  resource_group_name = azurerm_resource_group.cabstream.name
  location            = azurerm_resource_group.cabstream.location
  sku                 = "premium"

  tags = {
    project     = var.project_name
    owner       = var.owner_email
    environment = "dev"
    managed_by  = "terraform"
  }
}