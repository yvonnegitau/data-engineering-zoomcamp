locals {
data_lake_bucket = "de_data_lake"
}

variable "project" {
  description = "GDP PROJECT ID"
}

variable "region" {
  description= "Resource for GDP closest to location"
  default = "europe-west6"
  type = string
}
variable "storage_class" {
  description = "storage class type"
  default = "STANDARD"
}
variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be  written to"
  type = string
  default = "trips_data_all"
}

variable "BQ_TABLE" {
  description="BigQuery table"
  default = "ny_trips"
}