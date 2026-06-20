variable "subscription_id" {
  description = "Azure subscription ID"
  type        = string
}

variable "resource_group_name" {
  description = "Name of the resource group"
  type        = string
  default     = "cabstream-rg"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "East US"
}

variable "storage_account_name" {
  description = "ADLS Gen2 storage account name (must be globally unique, lowercase, 3-24 chars)"
  type        = string
  default     = "cabstreamdata"
}

variable "databricks_workspace_name" {
  description = "Databricks workspace name"
  type        = string
  default     = "cabstream-ws"
}

variable "eventhubs_namespace_name" {
  description = "Event Hubs namespace name"
  type        = string
  default     = "cabstream-eh"
}

variable "project_name" {
  description = "Project name for tagging"
  type        = string
  default     = "cabstream"
}

variable "owner_email" {
  description = "Owner email for tagging"
  type        = string
}


variable "client_id" {
  description = "Service principal client ID"
  type        = string
}

variable "client_secret" {
  description = "Service principal client secret"
  type        = string
  sensitive   = true
}

variable "tenant_id" {
  description = "Azure tenant ID"
  type        = string
}