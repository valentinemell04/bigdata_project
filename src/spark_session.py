"""SparkSession reliée au stockage S3 d'Onyxia (même réglage que le lab du cours 7)."""
import os
from pyspark.sql import SparkSession


def bucket() -> str:
    return os.environ.get("BUCKET", os.environ.get("KUBERNETES_NAMESPACE", ""))


def base_uri() -> str:
    """Racine du projet sur S3 : s3a://<bucket>/dvf"""
    return os.environ.get("DVF_BASE", f"s3a://{bucket()}/dvf")


def get_spark(app_name: str, extra: dict | None = None) -> SparkSession:
    b = SparkSession.builder.appName(app_name).master(os.environ.get("SPARK_MASTER", "local[*]")).config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4"))
    if "AWS_S3_ENDPOINT" in os.environ:
        b = (
            b.config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
            .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
            .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
            .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )
    for k, v in (extra or {}).items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
