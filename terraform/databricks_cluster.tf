# databricks_cluster.tf
# Databricks cluster, secret scope, and provider config
#
# Why this approach over manual UI cluster creation:
# - Auto-terminate is enforced in code, can't be forgotten between sessions
# - Survives apply/destroy/apply cycles automatically
# - Secret scope reads directly from existing Key Vault, no duplicated secrets
# - No manual ADLS mount step needed (mounts are legacy guidance anyway)
#   reads go straight through abfss:// using the secret scope

provider "databricks" {
  host                = azurerm_databricks_workspace.cabstream.workspace_url
  azure_client_id     = var.client_id
  azure_client_secret = var.client_secret
  azure_tenant_id     = var.tenant_id
}

resource "databricks_secret_scope" "cabstream_kv" {
  name = "cabstream-kv-scope"
  keyvault_metadata {
    resource_id = azurerm_key_vault.cabstream.id
    dns_name    = azurerm_key_vault.cabstream.vault_uri
  }
}

resource "databricks_cluster" "cabstream" {
  cluster_name            = "cabstream-cluster"
  spark_version           = "17.3.x-scala2.13"
  node_type_id            = "Standard_F4ads_v7"
  autotermination_minutes = 15
  num_workers             = 0
  data_security_mode      = "SINGLE_USER"
  single_user_name        = var.owner_email

  spark_conf = {
    "spark.databricks.cluster.profile"                        = "singleNode"
    "spark.master"                                            = "local[*]"
    "fs.azure.account.key.cabstreamdata.dfs.core.windows.net" = "{{secrets/cabstream-kv-scope/adls-storage-key}}"
  }

  azure_attributes {
    availability       = "SPOT_WITH_FALLBACK_AZURE"
    first_on_demand    = 1
    spot_bid_max_price = -1
  }

  custom_tags = {
    "ResourceClass" = "SingleNode"
  }
}



output "cluster_id" {
  value = databricks_cluster.cabstream.id
}