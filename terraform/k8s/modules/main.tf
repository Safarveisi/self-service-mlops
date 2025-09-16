resource "ionoscloud_datacenter" "k8s-self-service-mlops-platform-dc" {
  name                  = var.dc_name
  location              = var.location
  description           = "DataCenter for Challenge"
  sec_auth_protection   = false
}

resource "ionoscloud_lan" "k8s-self-service-mlops-platform-lan" {
  datacenter_id         = ionoscloud_datacenter.k8s-self-service-mlops-platform-dc.id
  public                = false
  name                  = var.lan_name
  lifecycle {
    create_before_destroy = true
  }
}

resource "ionoscloud_ipblock" "k8s-self-service-mlops-platform-ip" {
  location              = var.location
  size                  = var.ipblock_size
  name                  = var.ipblock_name
}

resource "ionoscloud_k8s_cluster" "k8s-self-service-mlops-platform-cluster" {
  name                  = var.cluster_name
  k8s_version           = var.k8s_version
}

resource "ionoscloud_k8s_node_pool" "k8s-self-service-mlops-platform-node_pool" {
  datacenter_id         = ionoscloud_datacenter.k8s-self-service-mlops-platform-dc.id
  k8s_cluster_id        = ionoscloud_k8s_cluster.k8s-self-service-mlops-platform-cluster.id
  name                  = "k8s-self-service-mlops-platform-node_pool"
  k8s_version           = ionoscloud_k8s_cluster.k8s-self-service-mlops-platform-cluster.k8s_version
  auto_scaling {
    min_node_count      = 4
    max_node_count      = 5
  }
  cpu_family            = "INTEL_SKYLAKE"
  availability_zone     = "AUTO"
  storage_type          = "SSD"
  node_count            = 4
  cores_count           = 8
  ram_size              = 12288
  storage_size          = 100
  public_ips            = [ ionoscloud_ipblock.k8s-self-service-mlops-platform-ip.ips[0], ionoscloud_ipblock.k8s-self-service-mlops-platform-ip.ips[1], ionoscloud_ipblock.k8s-self-service-mlops-platform-ip.ips[2], ionoscloud_ipblock.k8s-self-service-mlops-platform-ip.ips[3], ionoscloud_ipblock.k8s-self-service-mlops-platform-ip.ips[4], ionoscloud_ipblock.k8s-self-service-mlops-platform-ip.ips[5] ]
  lans {
    id                  = ionoscloud_lan.k8s-self-service-mlops-platform-lan.id
    dhcp                = true
    routes {
       network          = "192.168.10.0/24"
       gateway_ip       = "192.168.10.1"
     }
   }
}

data "ionoscloud_k8s_cluster" "k8s-self-service-mlops-platform-cluster" {
  depends_on = [ ionoscloud_k8s_node_pool.k8s-self-service-mlops-platform-node_pool ]
  name = var.cluster_name
}
resource "local_file" "kubeconfig" {
  content  = data.ionoscloud_k8s_cluster.k8s-self-service-mlops-platform-cluster.kube_config
  filename = "/home/${var.local_user}/.kube/ionos_kubeconf.yaml"
}

output "kubeConfig_location" {
  value = "*** Your kuberenets config file store in ~/.kube directory with name ionos_kubeconf.yaml ***"
}

