locals {
  data_lake_bucket = "dtc-data-lake"
}

variable "project" {
  description = "GCP Project ID"
  default = "train-delays-jpatel"
}

variable "region" {
  description = "us-west1"
  default = "us-west1"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "train_delays_data_all"
}
