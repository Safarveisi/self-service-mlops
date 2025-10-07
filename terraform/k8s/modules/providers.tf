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
#  we encourage users to use token authentication for security reasons
#  username          = var.ionos_username
#  password          = var.ionos_password
  token             = var.ionos_token
#  optional, to be used only for reseller accounts
#  contract_number = "contract_number_here"
#  optional, does not need to be configured in most cases
#  endpoint = "custom_cloud_api_url"
#  s3_access_key     =  "your_access_key"
#  s3_secret_key     =  "your_secret_key"

}
