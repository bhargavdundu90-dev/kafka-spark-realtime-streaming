# Deploying to AWS

The local stack maps one-to-one onto managed AWS services:

| Local (docker compose) | AWS | Why this choice |
|---|---|---|
| Kafka (KRaft)          | **Amazon MSK Serverless**, IAM auth | Same Kafka API, no broker sizing; Kinesis Data Streams is the alternative if the producers are already AWS-native |
| Spark container        | **EMR Serverless** (Spark 3.5, emr-7.x) | Long-running streaming job without managing a cluster. Glue Streaming works too but gives less control over Spark/Kafka versions |
| MinIO bucket `lake`    | **S3** (SSE-KMS, public access blocked) | Parquet, partitioned by date |
| —                      | **Glue Data Catalog + Athena** | `fraud_signals` table uses partition projection, so no crawler or `MSCK REPAIR` is needed |

Region defaults to `ap-south-1` (Mumbai) since card data for Indian issuers is expected to stay in India.

## 1. Infrastructure

```bash
cd terraform
terraform init
terraform apply -var vpc_id=vpc-xxxx -var 'subnet_ids=["subnet-a","subnet-b"]'
```

Creates: lake bucket, MSK Serverless cluster, EMR Serverless application, job IAM role
(least-privilege S3 + Kafka read), Glue database/table and an Athena workgroup with a 10 GB scan guardrail.

## 2. Package and upload the job

```bash
BUCKET=$(terraform -chdir=terraform output -raw lake_bucket)
(cd src && zip -r ../txnstream.zip txnstream)
aws s3 cp txnstream.zip       s3://$BUCKET/code/
aws s3 cp jobs/stream_job.py  s3://$BUCKET/code/
```

## 3. Start the streaming job

MSK Serverless only accepts IAM auth. Setting `MSK_IAM=true` makes `read_kafka()`
add the SASL_SSL / `AWS_MSK_IAM` options; the `aws-msk-iam-auth` package must be on the classpath.

```bash
aws emr-serverless start-job-run \
  --application-id $(terraform -chdir=terraform output -raw emr_application_id) \
  --execution-role-arn $(terraform -chdir=terraform output -raw emr_job_role_arn) \
  --mode STREAMING \
  --job-driver '{"sparkSubmit":{
      "entryPoint":"s3://'$BUCKET'/code/stream_job.py",
      "sparkSubmitParameters":"--py-files s3://'$BUCKET'/code/txnstream.zip --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,software.amazon.msk:aws-msk-iam-auth:2.2.0 --conf spark.emr-serverless.driverEnv.OUTPUT_PATH=s3://'$BUCKET' --conf spark.emr-serverless.driverEnv.CHECKPOINT_PATH=s3://'$BUCKET'/checkpoints --conf spark.emr-serverless.driverEnv.KAFKA_BOOTSTRAP=<msk-bootstrap>:9098 --conf spark.emr-serverless.driverEnv.MSK_IAM=true"}}'
```

`--mode STREAMING` makes EMR Serverless restart the job on failure; the S3
checkpoints let it resume from the last committed Kafka offsets.

## 4. Query

Open Athena, choose the `txnstream-analysts` workgroup and run `sql/athena_queries.sql`.

## Cost notes

- Stop the job and `terraform destroy` when not demoing; MSK Serverless and a running EMR job bill per hour.
- The quarantine prefix expires after 30 days via S3 lifecycle.
- Small files: with a 30 s trigger, schedule a daily compaction (Spark or Athena `CTAS`) on yesterday's partitions,
  or switch the sinks to Apache Iceberg and use `OPTIMIZE`.
