# eventhubs.tf
# Azure Event Hubs - managed Kafka on Azure
#
# Why Event Hubs over self-managed Kafka:
# - Zero infrastructure: no Docker, no Zookeeper, no cluster management
# - Kafka-compatible API: your existing producer code works unchanged
# - Just change bootstrap.servers to Event Hubs connection string
# - Scales automatically, no partition rebalancing to manage
# - Built-in message retention (1 day on Basic tier)
# - Fully managed: no ops overhead
#
# Why Basic tier:
# - 1 consumer group (enough for our pipeline)
# - 1 day retention (fine for streaming ingestion)
# - Cheapest: ~$0.015/million events
# - Production would use Standard tier for multiple consumer groups

resource "azurerm_eventhub_namespace" "cabstream" {
  name                = var.eventhubs_namespace_name
  location            = azurerm_resource_group.cabstream.location
  resource_group_name = azurerm_resource_group.cabstream.name
  sku                 = "Basic"
  capacity            = 1

  tags = {
    project     = var.project_name
    owner       = var.owner_email
    environment = "dev"
    managed_by  = "terraform"
  }
}

# Kafka topic equivalent: taxi-trips
# 3 partitions matches our local Kafka setup
# Same parallelism: 3 Spark workers can read simultaneously
resource "azurerm_eventhub" "taxi_trips" {
  name                = "taxi-trips"
  namespace_name      = azurerm_eventhub_namespace.cabstream.name
  resource_group_name = azurerm_resource_group.cabstream.name
  partition_count     = 3
  message_retention   = 1
}

# Authorization rule for producer and consumer
resource "azurerm_eventhub_namespace_authorization_rule" "cabstream" {
  name                = "cabstream-access"
  namespace_name      = azurerm_eventhub_namespace.cabstream.name
  resource_group_name = azurerm_resource_group.cabstream.name

  listen = true
  send   = true
  manage = false
}