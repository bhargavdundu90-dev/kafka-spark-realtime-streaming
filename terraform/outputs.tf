output "lake_bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "msk_cluster_arn" {
  value = aws_msk_serverless_cluster.this.arn
}

output "emr_application_id" {
  value = aws_emrserverless_application.spark.id
}

output "emr_job_role_arn" {
  value = aws_iam_role.job.arn
}

output "athena_workgroup" {
  value = aws_athena_workgroup.this.name
}
