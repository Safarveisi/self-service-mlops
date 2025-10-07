variable "ionos_token" {
  description = "Token for ionos cloud"
  type = string
  sensitive = true
}

variable "location" {
  description = "The location for the resources"
  type        = string
  default     = "de/txl"
}

variable "dc_name" {
  description = "Name for the datacenter"
  type        = string
  default     = "k8s-self-service-mlops-platform-dc"
}

variable "lan_name" {
  description = "Name for the LAN"
  type        = string
  default     = "k8s-self-service-mlops-platform-lan"
}

variable "ipblock_size" {
  description = "Size of the IP Block"
  type        = number
  default     = 8
}

variable "ipblock_name" {
  description = "Name of the IP Block"
  type        = string
  default     = "k8s-self-service-mlops-platform-ip"
}

variable "cluster_name" {
  description = "Name of the k8s cluster"
  type        = string
  default     = "k8s-self-service-mlops-platform-cluster"
}

variable "k8s_version" {
  description = "Version of Kubernetes to use"
  type        = string
  default     = "1.32.5"
}
