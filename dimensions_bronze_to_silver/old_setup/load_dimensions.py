from load_scd import load_scd2_dims
import csv
import os


input_src_table = 'product_catalog'
config_path='/home/vd/data-engineering/projects/Telecom/telecom-env/code/configs/config.csv'

aws_access_key_id = "JsvDopi1rRIyu4JccHKw"
aws_secret_access_key = "AqSee06JLSjUbYrUiQsAOiI6Ys3KwZmZClbU3Hoy"

with open(config_path, newline='') as csvfile:
    csvreader = csv.DictReader(csvfile)
    table_details = [row for row in csvreader 
                     if row['is_active'] == 'Y' 
                     and row['table_name'] == input_src_table 
                     and row['dim_or_fact'] == 'dim'
                    ][0]


    scd_type = table_details['scd1_or_scd2']

    if scd_type not in ['scd1', 'scd2']:
        raise ValueError("Invalid SCD Type")
    
    key_columns = table_details['key_columns']
    non_key_columns = table_details['non_key_columns']
    is_scd = table_details['is_scd']


    if (is_scd == 'Y') and (scd_type == 'scd2'):
        load_scd2_dims(input_src_table, key_columns, non_key_columns, aws_access_key_id, aws_secret_access_key)
        # print("Calling load_scd2")
    elif (is_scd == 'Y') and (scd_type == 'scd1'):
        pass
        # load_scd1_dims(input_src_table, key_columns, non_key_columns, aws_access_key_id, aws_secret_access_key)
    else:
        pass
        # load_overwrite_dims(input_src_table, aws_access_key_id, aws_secret_access_key)




