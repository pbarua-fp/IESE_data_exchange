# Databricks notebook source
# MAGIC %md This notebook is to write the dataframes in the given format to a destination path

# COMMAND ----------

# MAGIC %md #Import Libraries

# COMMAND ----------

# MAGIC %run /Shared/iese_data_exchange/01_utils/01_libraries/libraries

# COMMAND ----------

# MAGIC %md #Write to Delta files

# COMMAND ----------

def write_to_delta(df: DataFrame, mode: str, path = str)-> None:
    df.write.format('delta').mode(mode).save(path)

# COMMAND ----------

# MAGIC %md # Write to Parquet 

# COMMAND ----------

def write_to_parquet(df: DataFrame, mode: str, path = str)-> None:
    df.write.format('parquet').mode(mode).save(path)

# COMMAND ----------

# MAGIC %md #Write to CSV

# COMMAND ----------

def write_to_csv(df: DataFrame, mode: str, path = str)-> None:
    df.repartition(1).format('com.databricks.spark.csv').mode(mode).save(path)

# COMMAND ----------


