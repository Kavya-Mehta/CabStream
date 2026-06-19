# main.tf
# Configures the Azure provider and creates the resource group
# Why Terraform over click-ops:
# - Reproducible: one command recreates entire infrastructure
# - Version controlled: infrastructure changes reviewed like code
# - Destroyable: one command tears everything down, no orphaned resources

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
  required_version = ">= 1.0"
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

# Resource Group
# Everything in this project lives here
# Delete this = delete everything = no surprise bills
resource "azurerm_resource_group" "cabstream" {
  name     = var.resource_group_name
  location = var.location

  tags = {
    project     = var.project_name
    owner       = var.owner_email
    environment = "dev"
    managed_by  = "terraform"
  }
}