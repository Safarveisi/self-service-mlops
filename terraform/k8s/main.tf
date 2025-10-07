terraform {
  backend "s3" {
    bucket = "customerintelligence"
    key    = "self_service_mlops/terraform/terraform.tfstate"
    region = "eu-central-1"
    endpoints = {
      s3 = "http://s3-de-central.profitbricks.com"
    }
    skip_credentials_validation = true
    skip_requesting_account_id  = true
  }

}

module "k8s" {
  source = "./modules"
  ionos_token = var.ionos_token
}
