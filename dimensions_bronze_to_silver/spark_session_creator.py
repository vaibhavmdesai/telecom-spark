# spark_session_creator.py
from pyspark.sql import SparkSession


class SparkSessionCreator:
    @staticmethod
    def create_spark_session(aws_access_key_id: str, aws_secret_access_key: str) -> SparkSession:
        spark = (SparkSession.builder
        .appName("Spark S3 Connection")
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
        .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.1.12:9000")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        .getOrCreate()
        )

        return spark   
