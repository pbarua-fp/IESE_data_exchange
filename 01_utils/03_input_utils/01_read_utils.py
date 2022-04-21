# Databricks notebook source
# MAGIC %md This notebook contains reusable functions that can be used to read a source file.

# COMMAND ----------

# MAGIC %md import Libraries

# COMMAND ----------

# MAGIC %run /Shared/iese_data_exchange/01_utils/01_libraries/libraries

# COMMAND ----------

# MAGIC %md #Read CSV file with a pipe delimiter specified

# COMMAND ----------

def read_csv_1(path:str) -> DataFrame:
    df = (spark
          .read
          .format('com.databricks.spark.csv')
          .option('header', True)
          .option('delimiter', '|')
          .option('charset', 'iso-8859-1')
         )
    return (df.load(path))

# COMMAND ----------

# MAGIC %md 
# MAGIC # Read CSV file with infered schema

# COMMAND ----------

def read_csv(path:str, schema = StructType, options:dict ={}) ->DataFrame:
    df = (spark
         .read
         .format('csv')
         .schema(schema)
         )
    if options and len(options): # to check if there are any options provided
        df.options(**options)
        
    return(df.load(path))

# COMMAND ----------

# MAGIC %md # Read Delta file

# COMMAND ----------

def read_delta_table(path:str, options:dict ={}) ->DataFrame:
    df = (spark
         .read
         .format('delta')
         )
    if options and len(options): # to check if there are any options provided
        df.options(**options)
        
    return(df.load(path))

# COMMAND ----------


