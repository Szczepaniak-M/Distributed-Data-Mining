variable "key_pair" {
  type      = string
  sensitive = true
}

variable "image_id" {
  type = string
}

variable "instance_amount" {
  type = number
}

variable "volume_size" {
  type = number
}

variable "flavor_name" {
  type = string
}
