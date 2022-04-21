# Databricks notebook source
# MAGIC %run /Shared/iese_data_exchange/01_utils/01_libraries/libraries

# COMMAND ----------

# MAGIC %run /Shared/iese_data_exchange/01_utils/02_data_cleaning_utils/initial_reporting_unit_functions

# COMMAND ----------

# MAGIC %run /Shared/iese_data_exchange/01_utils/02_data_cleaning_utils/pre_processing_unit_functions

# COMMAND ----------

# MAGIC %run /Shared/iese_data_exchange/01_utils/03_input_utils/01_read_utils

# COMMAND ----------

# MAGIC %run /Shared/iese_data_exchange/01_utils/03_input_utils/02_json_config_utils

# COMMAND ----------

# MAGIC %run /Shared/iese_data_exchange/01_utils/04_output_utils/01_write_utils

# COMMAND ----------

# MAGIC %md ## Data loading

# COMMAND ----------

df_calls = read_csv_1("/tmp/synthetic-data/basic-synthetic/calls/")
display(df_calls)

# COMMAND ----------

# MAGIC %md ## Initial reporting

# COMMAND ----------

check_columns_exist(df_calls, ['callId'])
null_count_col_list(df_calls, col_list=['callId','callerNumber','receipientNumber','callDurationSeconds'])

# COMMAND ----------

# MAGIC %md ## Pre-processing

# COMMAND ----------


df_calls = hash_column_with_salt(df=df_calls, column_name='callerNumber', salt_value='1232456748')
df_calls = hash_column_with_salt(df=df_calls, column_name='receipientNumber', salt_value='1232456748')
df_calls = remove_spl_char_col(df_calls, ['callId'])
df_calls = cast_string_cols(df_calls, ['callDurationSeconds'])
df_calls = cast_int_cols(df_calls, ['callDurationSeconds'])

display(df_calls)

# COMMAND ----------

# MAGIC %md ## Save amended table to location

# COMMAND ----------

write_to_parquet(df=df_calls, mode='overwrite', path='/tmp/pre-processed/synthetic-data/basic-synthetic/calls/')
