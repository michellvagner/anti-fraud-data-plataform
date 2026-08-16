terraform {
    required_version = ">= 1.0"

    required_providers {
    aws = {
        source = "hashicorp/aws"
        }
    }
}

provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "data_lake" {
  bucket = "anti-fraud-data-platform"
  force_destroy = true
}

resource "aws_s3_object" "landing" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "landing/"
}

resource "aws_s3_object" "transactions" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "landing/transactions/"
}

resource "aws_s3_object" "raw" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "raw/"
}

resource "aws_s3_object" "raw_transactions" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "raw/transactions/"
}
