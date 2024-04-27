terraform {
  required_version = ">= 0.14.0"
  required_providers {
    openstack = {
      source  = "terraform-provider-openstack/openstack"
      version = "~> 1.53.0"
    }
  }
}

provider "openstack" {
  # All other properties taken from environmental variables
  enable_logging    = true
}

resource "openstack_blockstorage_volume_v3" "vol" {
  count    = var.instance_amount
  name     = "vol-${count.index}"
  size     = var.volume_size
  image_id = var.image_id
}

resource "openstack_compute_instance_v2" "hadoop_nodes" {
  count       = var.instance_amount
  name        = "hadoop-node-${count.index}"
  flavor_name = var.flavor_name
  image_id    = var.image_id
  key_pair    = var.key_pair
  network {
    name = "network"
  }

  block_device {
    uuid             = openstack_blockstorage_volume_v3.vol[count.index].id
    source_type      = "volume"
    boot_index       = 0
    destination_type = "volume"
  }

  security_groups = ["default"]
  depends_on = [
    openstack_blockstorage_volume_v3.vol
  ]
}

resource "openstack_networking_floatingip_v2" "float_ip" {
  count = var.instance_amount
  pool  = "network_pool"
}

resource "openstack_compute_floatingip_associate_v2" "fip_1" {
  count = var.instance_amount
  floating_ip = openstack_networking_floatingip_v2.float_ip[count.index].address
  instance_id = openstack_compute_instance_v2.hadoop_nodes[count.index].id
}

output "hadoop-node-ip" {
  value = [for index, float_ip in openstack_networking_floatingip_v2.float_ip : "hadoop-node-${index} ${float_ip.address}"]
}
