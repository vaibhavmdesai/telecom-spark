from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException


def create_spark_session(aws_access_key_id, aws_secret_access_key):

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
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        .getOrCreate()
    )

    return spark

def load_source_data(input_src_table, aws_access_key_id, aws_secret_access_key):
    
    spark = create_spark_session(aws_access_key_id, aws_secret_access_key)
    df = spark.read.format('parquet').load(f"s3a://oabronze/{input_src_table}/{input_src_table}.parquet")
    return {'df': df, 'spark': spark}
    

def load_scd2_dims(input_src_table, key_columns_str, non_key_columns_str, aws_access_key_id, aws_secret_access_key):

    target_table_path = f"s3a://oasilver/{input_src_table}"

    src_df = load_source_data(input_src_table, aws_access_key_id, aws_secret_access_key)['df']
    spark = load_source_data(input_src_table, aws_access_key_id, aws_secret_access_key)['spark']
    print("INPUT DATA FRAME")
    src_df.show()
    key_columns = key_columns_str.split(',')
    non_key_columns = non_key_columns_str.split(',')

    input_src_dataframe = src_df.withColumn("effective_start_date", F.current_date()) \
        .withColumn("effective_end_date", F.lit('9999-12-31').cast('date')) \
        .withColumn("active_flag", F.lit(1))
    
    input_src_dataframe.show(10, False)

    try:
        delta_table = DeltaTable.forPath(spark, target_table_path)

    except AnalysisException:
        # If the table doesn't exist, create it by writing the input data to the target path
        print(f"Delta table not found at {target_table_path}. Creating new table...")   
        input_src_dataframe.write.format("delta").mode("overwrite").save(target_table_path)
        delta_table = DeltaTable.forPath(spark, target_table_path)


    # Below code will update the flag, effective_end_date for existing records and also add the rows for non-existing keys
    delta_table.alias("target").merge(
        input_src_dataframe.alias("source"),
        condition= " AND ".join([f"target.{key} = source.{key}" for key in key_columns])  # Matching key columns
    ).whenMatchedUpdate(
        condition="(" + " OR ".join([
            f"target.{non_key} != source.{non_key}" for non_key in non_key_columns  # Check if non-key columns have changed
        ]) + ") AND target.active_flag = 1",  # Ensure we update only active records
        set={
            "target.effective_end_date": F.current_date(),  # Set the effective_end_date for old records
            "target.active_flag": F.lit(0)  # Mark old records as inactive
        }
    ).whenNotMatchedInsert(
        values={
            key: F.col(f"source.{key}") for key in key_columns  # Insert the key columns
        } | {
            non_key: F.col(f"source.{non_key}") for non_key in non_key_columns  # Insert the non-key columns
        } | {
            "effective_start_date": F.current_date(),  # Set the start date for the new records
            "effective_end_date": F.lit('9999-12-31').cast('date'),  # New records have no end date
            "active_flag": F.lit(1)  # Set the active flag to 1 for new records
        }
    ).execute()


    # Below code is to insert the records which are already existing in target but have changed.
    delta_table.alias("target").merge(
        input_src_dataframe.alias("source"),
        condition= " AND ".join([f"target.{key} = source.{key}" for key in key_columns]) + " AND target.active_flag = 1"  # Matching key columns
    ).whenNotMatchedInsert(
        values={
            key: F.col(f"source.{key}") for key in key_columns  # Insert the key columns
        } | {
            non_key: F.col(f"source.{non_key}") for non_key in non_key_columns  # Insert the non-key columns
        } | {
            "effective_start_date": F.current_date(),  # Set the start date for the new records
            "effective_end_date": F.lit('9999-12-31').cast('date'),  # New records have no end date
            "active_flag": F.lit(1)  # Set the active flag to 1 for new records
        }
    ).execute()




# def load_scd1_dims(input_src_table, key_columns, non_key_columns, aws_access_key_id, aws_secret_access_key):
#     print(f"SCD1 for {input_src_table}")

# def load_overwrite_dims(input_src_table, aws_access_key_id, aws_secret_access_key):
#     print(f"Overwriting table {input_src_table}")


# 1. Read the input param for table name

# 2. Read the other configs of the table from the config file.

# 3. Read the source table as src_df based on the parameter.

# 4. Add the new columns effective_start_dt, effective_end_dt, active_flag to the src_df

# 5. Check if the silver delta table already exists.

# 6. If the record already exists mark the effective_end_dt, active_flag.

# 7. Insert the new records in delta table.