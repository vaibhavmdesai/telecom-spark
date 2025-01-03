from pyspark.sql import SparkSession
import pyspark.sql.functions as F

aws_access_key_id = "JsvDopi1rRIyu4JccHKw"
aws_secret_access_key = "AqSee06JLSjUbYrUiQsAOiI6Ys3KwZmZClbU3Hoy"

spark = (
        SparkSession.builder
        .appName("Spark S3 Connection")
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
        .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.1.12:9000")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")


# ddf = spark.read.format('delta').load("s3a://oasilver/product_catalog/")
# ddf.show(100, False)
# ddf.createOrReplaceTempView("deltavw")

pdf = spark.read.format('parquet').load("s3a://oabronze/product_catalog/product_catalog.parquet")
pdf.show(100, False)
# pdf.createOrReplaceTempView("parquetvw")
