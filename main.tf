# Configure Azure Provider
provider "azurerm" {
  features {}
}

# Create Azure Resource Group
resource "azurerm_resource_group" "rg" {
  name = "Reddit-api-arul"
  location = "UK South"
}

# Create Azure Blob Storage Account
resource "azurerm_storage_account" "data_storage" {
  name = "redditsaarul123"
  resource_group_name = azurerm_resource_group.rg.name
  location = azurerm_resource_group.rg.location
  account_tier = "Standard"
  account_replication_type = "LRS"
}
