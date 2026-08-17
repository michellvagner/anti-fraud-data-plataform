resource "aws_s3_object" "glue_script" {

  bucket = aws_s3_bucket.data_lake.id

  key = "scripts/csv_to_parquet.py"

  source = "${path.module}/../src/glue/csv_to_parquet.py"

  etag = filemd5(
    "${path.module}/../src/glue/csv_to_parquet.py"
  )
}

resource "aws_glue_job" "csv_to_parquet" {

  name = "anti-fraud-csv-to-parquet"

  role_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/LabRole"

  glue_version = "5.0"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/csv_to_parquet.py"
    python_version  = "3"
  }

  worker_type       = "G.1X"
  number_of_workers = 2

  default_arguments = {
    "--job-language" = "python"
  }

  depends_on = [
    aws_s3_object.glue_script
  ]
}