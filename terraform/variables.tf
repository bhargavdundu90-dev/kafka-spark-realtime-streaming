variable "region" {
  type    = string
  default = "ap-south-1" # Mumbai: keeps card data in India (RBI data-localisation)
}

variable "owner" {
  type    = string
  default = "data-engineering"
}

variable "name" {
  type    = string
  default = "txnstream"
}

variable "subnet_ids" {
  description = "Private subnets (at least 2 AZs) for MSK Serverless and EMR Serverless"
  type        = list(string)
}

variable "vpc_id" {
  type = string
}
