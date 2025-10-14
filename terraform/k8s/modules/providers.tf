terraform {

  required_providers {
    ionoscloud = {
      source = "ionos-cloud/ionoscloud"
      version = "6.7.14"
    }
    external = {
      source = "hashicorp/external"
      version = "2.3.1"
    }
    local = {
      source = "hashicorp/local"
      version = "2.4.0"
    }
    kubernetes = {
      source = "hashicorp/kubernetes"
      version = "2.21.1"
    }
  }
}

provider "ionoscloud" {
# Use token authentication for security reasons
  token             = var.ionos_token
}
