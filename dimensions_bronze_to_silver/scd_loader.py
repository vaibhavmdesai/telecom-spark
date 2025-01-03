# scd_loader.py
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from dimensions_bronze_to_silver.spark_session_creator import SparkSessionCreator

class SCDLoader:
    def load(self, input_src_table: str, key_columns_str: str, non_key_columns_str: str, aws_access_key_id: str, aws_secret_access_key: str):
        pass


class SCD2Loader(SCDLoader):
    def load(self, input_src_table: str, key_columns_str: str, non_key_columns_str: str, aws_access_key_id: str, aws_secret_access_key: str):
        target_table_path = f"s3a://oasilver/{input_src_table}"

        # Load source data
        src_df, spark = self.load_source_data(input_src_table, aws_access_key_id, aws_secret_access_key)

        key_columns = key_columns_str.split(',')
        non_key_columns = non_key_columns_str.split(',')

        input_src_dataframe = src_df.withColumn("effective_start_date", F.current_date()) \
            .withColumn("effective_end_date", F.lit('9999-12-31').cast('date')) \
            .withColumn("active_flag", F.lit(1))

        try:
            delta_table = DeltaTable.forPath(spark, target_table_path)
        except AnalysisException:
            # If the table doesn't exist, create it by writing the input data to the target path
            print(f"Delta table not found at {target_table_path}. Creating new table...")
            input_src_dataframe.write.format("delta").mode("overwrite").save(target_table_path)
            delta_table = DeltaTable.forPath(spark, target_table_path)

        # Merge logic for SCD2
        delta_table.alias("target").merge(
            input_src_dataframe.alias("source"),
            condition=" AND ".join([f"target.{key} = source.{key}" for key in key_columns])
        ).whenMatchedUpdate(
            condition="(" + " OR ".join([f"target.{non_key} != source.{non_key}" for non_key in non_key_columns]) + ") AND target.active_flag = 1",
            set={
                "target.effective_end_date": F.current_date(),
                "target.active_flag": F.lit(0)
            }
        ).whenNotMatchedInsert(
            values={
                key: F.col(f"source.{key}") for key in key_columns
            } | {
                non_key: F.col(f"source.{non_key}") for non_key in non_key_columns
            } | {
                "effective_start_date": F.current_date(),
                "effective_end_date": F.lit('9999-12-31').cast('date'),
                "active_flag": F.lit(1)
            }
        ).execute()

        # Insert records which changed
        delta_table.alias("target").merge(
            input_src_dataframe.alias("source"),
            condition=" AND ".join([f"target.{key} = source.{key}" for key in key_columns]) + " AND target.active_flag = 1"
        ).whenNotMatchedInsert(
            values={
                key: F.col(f"source.{key}") for key in key_columns
            } | {
                non_key: F.col(f"source.{non_key}") for non_key in non_key_columns
            } | {
                "effective_start_date": F.current_date(),
                "effective_end_date": F.lit('9999-12-31').cast('date'),
                "active_flag": F.lit(1)
            }
        ).execute()

    def load_source_data(self, input_src_table: str, aws_access_key_id: str, aws_secret_access_key: str) -> (DataFrame, SparkSession): # type: ignore
        spark = SparkSessionCreator.create_spark_session(aws_access_key_id, aws_secret_access_key)
        df = spark.read.parquet(f"s3a://oabronze/{input_src_table}/{input_src_table}.parquet")
        return df, spark


class SCD1Loader(SCDLoader):
    def load(self, input_src_table: str, key_columns_str: str, non_key_columns_str: str, aws_access_key_id: str, aws_secret_access_key: str):
        print(f"SCD1 loading logic for {input_src_table}")
        # Logic for SCD1, can be implemented when needed


class OverwriteLoader(SCDLoader):
    def load(self, input_src_table: str, aws_access_key_id: str, aws_secret_access_key: str):
        print(f"Overwriting table {input_src_table}")
        # Logic for overwriting data in a table, can be implemented when needed
