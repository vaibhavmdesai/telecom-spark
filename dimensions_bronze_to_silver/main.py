# main.py
import os
import csv
import sys
from dimensions_bronze_to_silver.scd_loader_factory import SCDLoaderFactory


def main():
    input_src_table = 'product_catalog'
    config_path = '/home/vd/data-engineering/projects/Telecom/telecom-env/code/configs/config.csv'

    aws_access_key_id = os.getenv("AWS_ACCESS_KEY")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    with open(config_path, newline='') as csvfile:
        csvreader = csv.DictReader(csvfile)
        table_details = [row for row in csvreader
                         if row['is_active'] == 'Y'
                         and row['table_name'] == input_src_table
                         and row['dim_or_fact'] == 'dim'][0]

        scd_type = table_details['scd1_or_scd2']

        if scd_type not in ['scd1', 'scd2']:
            raise ValueError("Invalid SCD Type")

        key_columns = table_details['key_columns']
        non_key_columns = table_details['non_key_columns']
        is_scd = table_details['is_scd']

        # Get the correct loader based on the SCD type
        loader = SCDLoaderFactory.get_loader(scd_type)

        if is_scd == 'Y':
            loader.load(input_src_table, key_columns, non_key_columns, aws_access_key_id, aws_secret_access_key)
        else:
            print(f"Table {input_src_table} is not an SCD table. Skipping loading.")


if __name__ == "__main__":
    sys.path.insert(0, '/home/vd/data-engineering/projects/Telecom/telecom-env/code/spark/dimensions_bronze_to_silver')
    main()
