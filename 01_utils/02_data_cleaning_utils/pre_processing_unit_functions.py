# Databricks notebook source
# MAGIC %md This notebook is to create reusable functions that are for the pre-processing of the data. They comprise of :
# MAGIC 
# MAGIC     1. stemming
# MAGIC     2. Truncation
# MAGIC     3. regex
# MAGIC     4. datet function
# MAGIC     5. float to decimal Type
# MAGIC     6. Hashing with salt
# MAGIC     7. suppression of smaller groups
# MAGIC     8. hourly aggregation
# MAGIC     9. mapping

# COMMAND ----------

# MAGIC %md #import Libraries

# COMMAND ----------

# MAGIC %run /Shared/iese_data_exchange/01_utils/01_libraries/libraries

# COMMAND ----------

# MAGIC %md #stemming

# COMMAND ----------

#sample dataframe#

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data2 = [("James","","Smith","36636","M",3000, "2000/01/01", 'www.intentHq.com' ),
    ("Michael","Rose","","40288","M",4000, "1999/06/23", 'www.intenthq.com'),
    ("Robert","","Williams","42114","M",None, "2001/07/04" , 'https://www.intenthq.com' ),
    ("Maria","Anne","Jones","39192","F",4000, "1995/08/07", 'intentHQ'  ),
    ("Jen","Mary","Brown","","F",-1, "1996/09/08", 'INTENT' )
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True), 
    StructField("date_of_birth", StringType(), True), \
    StructField("company", StringType(), True)                 
  ])
 
df_stem = spark.createDataFrame(data=data2,schema=schema)


# COMMAND ----------

def convert_dict_to_df_stem(inp_dict, col_names_list, input_df : pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Converts dictionary format to Spark dataFrame
    replaces the company column of actual data set with column2 value of the dictionary data frame. 
    """
    pd_df = pd.DataFrame(list(inp_dict.items()))
    pd_df.columns = col_names_list
    spark = SparkSession.builder.getOrCreate()
    spark_df = spark.createDataFrame(pd_df)
    condition = (spark_df.column1 == input_df.company ) # replace 'company' with the right column from actual data set
    stem_df = (input_df
               .join(spark_df, on = (condition))
               .drop('company','column1') # replace 'company' with the right column from actual data set
               .withColumnRenamed('column2', 'company')  # replace 'company' with the right column from actual data set
              )
    return stem_df


# COMMAND ----------

#calling the function and verifying the output

inp_dict = {'www.intentHq.com' : 'intenthq.com',
             'www.intenthq.com' : 'intenthq.com',
             'https://www.intenthq.com' : 'intenthq.com',
             'intentHQ' : 'intenthq.com'}

col_names_list =['column1', 'column2']


stemmed = convert_dict_to_df_stem(inp_dict, col_names_list, df_stem)
display(stemmed)

# COMMAND ----------

# import pyspark.sql.functions as F
# import pyspark
# df = df_stem
# col_list = ['company']
# dict_col = {'www.intentHq.com' : 'intenthq.com',
#              'www.intenthq.com' : 'intenthq.com',
#              'https://www.intenthq.com' : 'intenthq.com',
#              'intentHQ' : 'intenthq.com'}
# for c in col_list:
#     df1 = df.withColumn(c, F.regexp_replace(F.col(c), c,  dict_col.get(c[0], c[1])))
    
# display(df1)

# COMMAND ----------

# MAGIC %md #regex / truncation

# COMMAND ----------

def remove_spl_char_col(df : pyspark.sql.DataFrame, col_list: list) ->pyspark.sql.DataFrame:
    
    for c in col_list:
        df = df.withColumn(c, F.trim(F.regexp_replace(F.trim(F.upper(F.col(c))), "[^a-zA-Z0-9]", "")))
        return df

# COMMAND ----------

# MAGIC %md #Date function

# COMMAND ----------

def cast_date_cols(df : pyspark.sql.DataFrame, col_list, ts_format):
    """
    cast multiple date columns in string type into its date type.
    """
    if isinstance(ts_format, str):
        format_len = len(ts_format)
        for c in col_list:
            df = df.withColumn(c, F.to_date(F.col(c).substr(0, format_len), ts_format))
    else:
        for c in col_list:
            df = (df
                  .withColumn(c, F.coalesce(
                      F.to_date(F.col(c).substr(0, len(ts_format[0])), ts_format[0]),
                      F.to_date(F.col(c).substr(0, len(ts_format[1])), ts_format[1])))
                 )
            
    return df

# COMMAND ----------

# MAGIC %md #Float to Decimal 

# COMMAND ----------

def cast_float_cols(df : pyspark.sql.DataFrame, col_list):
    """
    cast multiple columns in string type into its decimal type.
    """
    for c in col_list:
        df = df.withColumn(c, (F.col(c).cast(DecimalType())))
               
    return df

# COMMAND ----------

# MAGIC %md #Hashing with salt

# COMMAND ----------

def hash_column_with_salt(df: pyspark.sql.DataFrame, column_name: str, salt_value: str, sha_length: int = 256):
    """
    Hashes the specified column in place and returns the reference to the dataframe.
    Returns ValueError if SHA bit length is not properly specified.
    For null values, also returns null, even when the salt is used.
    """
    if sha_length not in [224, 256, 384, 512]:
            raise ValueError(
                "Incorrect SHA bit length specified.  Lengths must be one of [224,256,384,512] and specified as int."
            )
    df = df.withColumn(
                column_name,
                F.sha2(F.concat(F.col(column_name), F.lit(salt_value)), sha_length),
            )
    return df

# COMMAND ----------

# MAGIC %md #suppression of smaller groups

# COMMAND ----------

def suppression_of_smaller_groups(df : pyspark.sql.DataFrame, col_list:list) -> pyspark.sql.DataFrame:
    
    for c in col_list:
        count_df = df.groupBy(c).count().select(c, F.col('count').alias('count_of_units'))
        supp_grp = (count_df.withColumn('groups', \
                                        F.when(F.col('count_of_units')<=3, \
                                               F.lit('small_groups'))\
                                        .otherwise(F.lit('large_groups')))
                   )
        filtered_df = supp_grp.filter(F.col('groups').isin('large_groups'))
        
        return filtered_df
    
   



# COMMAND ----------

# MAGIC %md #hourly aggregation

# COMMAND ----------

def hourly_aggregation(df : pyspark.sql.DataFrame, ts_col_list: list, ts_format: str, order_cols:list ) -> pyspark.sql.DataFrame:
    
    if isinstance(ts_format, str):
        format_len = len(ts_format)
        for c in ts_col_list:
            df = df.withColumn(c, F.to_timestamp(F.col(c).substr(0, format_len), ts_format))
    
            hour_col = df.withColumn('hour_unit', F.hour(F.col(c)))
            
            partition = hour_col.select('hour_unit')
            
            partition_cols = partition.columns
            
            for c in col_list:
                count_df = hour_col.groupBy(*partition_cols).count().select(c, F.col('count').alias('count_of_units'))
            
            return count_df


# COMMAND ----------

# MAGIC %md # get year of a date column

# COMMAND ----------

def get_year(df : pyspark.sql.DataFrame, 
              ts_col_list: list, 
              ts_format: str) -> pyspark.sql.DataFrame:
        
        if isinstance(ts_format, str):
            format_len = len(ts_format)
            for c in ts_col_list:
                df = df.withColumn(c, F.to_timestamp(F.col(c).substr(0, format_len), ts_format))
                year = df.withColumn('year', F.year(F.col(c)))
                
                
                return year

# COMMAND ----------

# MAGIC %md # get month of a date column

# COMMAND ----------

def get_month(df : pyspark.sql.DataFrame, 
              ts_col_list: list, 
              ts_format: str) -> pyspark.sql.DataFrame:
        
        if isinstance(ts_format, str):
            format_len = len(ts_format)
            for c in ts_col_list:
                df = df.withColumn(c, F.to_timestamp(F.col(c).substr(0, format_len), ts_format))
                month = df.withColumn('month', F.month(F.col(c)))
                
                
                return month

# COMMAND ----------

# MAGIC %md # get day of a date column

# COMMAND ----------

def get_day(df : pyspark.sql.DataFrame, 
              ts_col_list: list, 
              ts_format: str) -> pyspark.sql.DataFrame:
        
        if isinstance(ts_format, str):
            format_len = len(ts_format)
            for c in ts_col_list:
                df = df.withColumn(c, F.to_timestamp(F.col(c).substr(0, format_len), ts_format))
                day = df.withColumn('day', date_format(F.col(c), 'EEEE'))
                
                
                return day

# COMMAND ----------

# MAGIC %md # get hour of a date column

# COMMAND ----------

def get_hour(df : pyspark.sql.DataFrame, 
              ts_col_list: list, 
              ts_format: str) -> pyspark.sql.DataFrame:
        
        if isinstance(ts_format, str):
            format_len = len(ts_format)
            for c in ts_col_list:
                df = df.withColumn(c, F.to_timestamp(F.col(c).substr(0, format_len), ts_format))
                hour = df.withColumn('hour', F.hour(F.col(c)))
                
                
                return hour
