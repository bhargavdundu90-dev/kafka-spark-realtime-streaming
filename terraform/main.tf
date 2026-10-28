data "aws_caller_identity" "me" {}

locals {
  bucket = "${var.name}-lake-${data.aws_caller_identity.me.account_id}-${var.region}"
}

# ---------- S3 data lake ----------
resource "aws_s3_bucket" "lake" {
  bucket = local.bucket
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "quarantine-expiry"
    status = "Enabled"
    filter {
      prefix = "quarantine/"
    }
    expiration {
      days = 30
    }
  }
}

# ---------- Network ----------
resource "aws_security_group" "stream" {
  name   = "${var.name}-stream"
  vpc_id = var.vpc_id

  ingress {
    description = "Kafka IAM auth inside the SG"
    from_port   = 9098
    to_port     = 9098
    protocol    = "tcp"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ---------- Ingestion: MSK Serverless (IAM auth) ----------
resource "aws_msk_serverless_cluster" "this" {
  cluster_name = "${var.name}-msk"

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = [aws_security_group.stream.id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }
}

# ---------- Processing: EMR Serverless (Spark 3.5) ----------
resource "aws_emrserverless_application" "spark" {
  name          = "${var.name}-spark"
  release_label = "emr-7.1.0"
  type          = "SPARK"

  maximum_capacity {
    cpu    = "16 vCPU"
    memory = "64 GB"
  }

  network_configuration {
    subnet_ids         = var.subnet_ids
    security_group_ids = [aws_security_group.stream.id]
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }
}

data "aws_iam_policy_document" "emr_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "job" {
  name               = "${var.name}-emr-job"
  assume_role_policy = data.aws_iam_policy_document.emr_assume.json
}

data "aws_iam_policy_document" "job" {
  statement {
    sid       = "Lake"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
  }
  statement {
    sid = "KafkaRead"
    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:ReadData",
      "kafka-cluster:DescribeGroup",
      "kafka-cluster:AlterGroup",
    ]
    resources = [
      aws_msk_serverless_cluster.this.arn,
      "arn:aws:kafka:${var.region}:${data.aws_caller_identity.me.account_id}:topic/${var.name}-msk/*",
      "arn:aws:kafka:${var.region}:${data.aws_caller_identity.me.account_id}:group/${var.name}-msk/*",
    ]
  }
  statement {
    sid       = "Catalog"
    actions   = ["glue:GetDatabase", "glue:GetTable", "glue:GetPartitions", "glue:CreatePartition", "glue:BatchCreatePartition"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "job" {
  role   = aws_iam_role.job.id
  policy = data.aws_iam_policy_document.job.json
}

# ---------- Serving: Glue Data Catalog + Athena ----------
resource "aws_glue_catalog_database" "this" {
  name = "${var.name}_db"
}

resource "aws_glue_catalog_table" "fraud_signals" {
  name          = "fraud_signals"
  database_name = aws_glue_catalog_database.this.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"                = "parquet"
    "projection.enabled"            = "true"
    "projection.window_date.type"   = "date"
    "projection.window_date.format" = "yyyy-MM-dd"
    "projection.window_date.range"  = "2026-01-01,NOW"
    "storage.location.template"     = "s3://${aws_s3_bucket.lake.bucket}/fraud_signals/window_date=$${window_date}/"
  }

  partition_keys {
    name = "window_date"
    type = "date"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/fraud_signals/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    columns {
      name = "card_id"
      type = "string"
    }
    columns {
      name = "window_start"
      type = "timestamp"
    }
    columns {
      name = "window_end"
      type = "timestamp"
    }
    columns {
      name = "txn_count"
      type = "bigint"
    }
    columns {
      name = "total_amount"
      type = "double"
    }
    columns {
      name = "distinct_cities"
      type = "int"
    }
    columns {
      name = "atm_count"
      type = "int"
    }
    columns {
      name = "small_ecom_count"
      type = "int"
    }
    columns {
      name = "signals"
      type = "array<string>"
    }
    columns {
      name = "risk_score"
      type = "int"
    }
    columns {
      name = "is_flagged"
      type = "boolean"
    }
  }
}

resource "aws_athena_workgroup" "this" {
  name = "${var.name}-analysts"
  configuration {
    enforce_workgroup_configuration = true
    bytes_scanned_cutoff_per_query  = 10737418240 # 10 GB guardrail
    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"
    }
  }
}
