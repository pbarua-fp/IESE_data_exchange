# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",None),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC 
# MAGIC %run /Shared/iese_data_exchange/01_utils/01_libraries/libraries

# COMMAND ----------

# MAGIC %run /Shared/iese_data_exchange/01_utils/02_data_cleaning_utils/initial_reporting_unit_functions

# COMMAND ----------

def test_functions_string(inputDf: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """test the initial reporing function"""
    col_list = ['salary']
    string_cols = cast_string_cols(inputDf, col_list)
    return string_cols

# COMMAND ----------

string_cols = test_functions_string(df)
assert [i[1] for i in string_cols.dtypes if i[0] == 'salary'][0] == 'string'
display(string_cols)

# COMMAND ----------

def test_functions_int(inputDf: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """test the initial reporing function"""
    col_list = ['salary']
    int_cols = cast_int_cols(inputDf, col_list)
    return int_cols

# COMMAND ----------

int_cols = test_functions_int(string_cols)
assert [i[1] for i in int_cols.dtypes if i[0] == 'salary'][0] == 'int'
display(int_cols)

# COMMAND ----------

def test_functions_float(inputDf: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """test the initial reporing function"""
    col_list = ['salary']
    float_cols = cast_float_cols(inputDf, col_list)
    return float_cols

# COMMAND ----------

float_cols = test_functions_float(int_cols)
assert [i[1] for i in float_cols.dtypes if i[0] == 'salary'][0] == 'decimal(10,0)'
display(float_cols)

# COMMAND ----------

def cardinality(df : pyspark.sql.DataFrame, col_list:list) -> pyspark.sql.DataFrame:
    """
    gives a value in a list of the count of distinct values of a dataframe.
    """
    
    for c in col_list:
        drop_dup = df.dropDuplicates()
        count_df = drop_dup.groupBy(c).count().select(c, F.col('count').alias('count_of_units'))
    
    return count_df

# COMMAND ----------

def get_avg_rows( unfiltered_df : pyspark.sql.DataFrame,
                              partition_cols : list,
                              order_cols: list,
                              avg_col_list: list
                ) -> pyspark.sql.DataFrame:
    """
    gets the rows using window function to get average.
    """
    for c in avg_col_list:
        w= Window.partitionBy(*partition_cols).orderBy([F.asc(col) for col in order_cols])
        filtered_df = (unfiltered_df
                   .withColumn('avg', F.row_number().over(w)).agg({c: 'avg'})
                   .drop('avg')
                  )
        return filtered_df

# COMMAND ----------

partition_cols = ['gender']
order_cols = ['id']
avg_col_list = ['salary']

df1 = get_avg_rows(df,  
                  partition_cols,order_cols, avg_col_list)
display(df1)

# COMMAND ----------

col_list = ['salary']
group_col_list = ['gender']

df1 = median_value(df, 
                  col_list, group_col_list)
display(df1)

# COMMAND ----------

def min_value(df : pyspark.sql.DataFrame, col_list, group_col_list) -> pyspark.sql.DataFrame:
    """
    get minimum value for a group of columns.
    """
    for c in col_list:
        for i in group_col_list:
            df = (df
                  .groupBy(F.col(i))
                  .agg({c: 'min'})
                 )
               
    return df

# COMMAND ----------

def test_functions_min(inputDf: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """test the initial reporing function"""
    
    all_cols = inputDf.columns
    col_list = ['salary']
    group_col_list =['gender']
    min_value_df = min_value(inputDf, col_list, group_col_list)
    return min_value_df

# COMMAND ----------

df.show()

# COMMAND ----------

min_value1 = test_functions_min(df)
output_rows = [row for row in min_value1.collect()]
assert [i for i in output_rows if i.gender == 'M'][0]['min(salary)'] == 3000 
assert [i for i in output_rows if i.gender == 'F'][0]['min(salary)'] == -1 
display(min_value1)

# COMMAND ----------

def test_functions_max(inputDf: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """test the initial reporing function"""
    col_list = ['salary']
    group_col_list =['gender']
    max_value_df = max_value(inputDf, col_list, group_col_list)
    return max_value_df

# COMMAND ----------

max_value1 = test_functions_max(df)
display(max_value1)
output_rows = [row for row in max_value1.collect()]
assert [i for i in output_rows if i.gender == 'M'][0]['max(salary)'] == 4000 
assert [i for i in output_rows if i.gender == 'F'][0]['max(salary)'] == 4000

# COMMAND ----------

def test_functions_min1(inputDf: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """test the initial reporing function"""
    order_cols = ['salary']
    partition_cols =['gender']
    min_value_df1 = get_min_rows(inputDf, partition_cols, order_cols)
    return min_value_df1

# COMMAND ----------

min_value11 = test_functions_min1(df)
display(min_value11)

# COMMAND ----------

def test_functions_max1(inputDf: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """test the initial reporing function"""
    order_cols = ['salary']
    partition_cols =['gender']
    max_value_df1 = get_max_rows(inputDf, partition_cols, order_cols)
    return max_value_df1

# COMMAND ----------

max_value11 = test_functions_max1(df)
display(max_value11)

# COMMAND ----------

count_df = cardinality(df)
count_df



# COMMAND ----------

display(df)

# COMMAND ----------

def convert_null_to_false(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    converts null to False for all boolean columns.
    """
    bool_cols = [name for name, dtype in df.types if dtype == 'boolean']
    
    return df.fillna(False, subset = bool_cols)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

def test_nulls_boolean(input_df : pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    col_list = ['salary']
    nulls_bool_df = convert_null_to_false(df, col_list)
    return nulls_bool_df

# COMMAND ----------

def is_col_empty_or_not(input_df : pyspark.sql.DataFrame, col_list : list) -> pyspark.sql.DataFrame:
    """
    To check if the values are null or not and pass boolean values for null values.
    """
    for c in col_list:
        df1 = df.withColumn(c, F.when(F.col(c).isNotNull()) & (F.col(c) != ''), F.lit(False).otherwise(F.lit(True)))
        return df1

# COMMAND ----------

def test_nulls_check(input_df : pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    col_list = ['pass']
    nulls_bool_df = is_col_empty_or_not(input_df, col_list)
    return nulls_bool_df

# COMMAND ----------

nulls_check_Df = test_nulls_check(df_bool)
# display(nulls_check_Df)

# COMMAND ----------

nulls_bool_Df = convert_null_to_false(df)
display(nulls_bool_Df)

# COMMAND ----------

import pyspark.sql.types  as types

# COMMAND ----------

from pyspark.sql.types import *
def convert_null_to_false(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    converts null to False for all boolean columns.
    """
    bool_cols = [name for name, dtype in df.types if dtype == 'boolean']
    
    return df.fillna(False, subset = bool_cols)

# COMMAND ----------

def test_nulls_double(input_df : pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    
    nulls_df = convert_null_to_false(input_df)
    return nulls_df

# COMMAND ----------

display(df_bool)

# COMMAND ----------

nulls_double_Df = test_nulls_double(df_bool)

# COMMAND ----------

display(nulls_double_Df)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data2 = [("James","","Smith&","36 636","M",3000),
    ("Michael","Rose","","402 88","M",4000),
    ("Robert","","Williams¬","42 114","M",None ),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) 

  ])
 
df_regex = spark.createDataFrame(data=data2,schema=schema)
df_regex.printSchema()
df_regex.show(truncate=False)

# COMMAND ----------



# COMMAND ----------

def remove_spl_char_col(df : pyspark.sql.DataFrame, col_list: list) ->pyspark.sql.DataFrame:
    
    for c in col_list:
        df = df.withColumn(c, F.trim(F.regexp_replace(F.trim(F.upper(F.col(c))), "[^a-zA-Z0-9]", "")))
        return df

# COMMAND ----------

def test_regex(input_df : pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    col_list = ['lastname']
    filtered_df = remove_spl_char_col(input_df, col_list)
    return filtered_df

# COMMAND ----------

filter_df = test_regex(df_regex)
display(filter_df)

# COMMAND ----------

def truncation(df:pyspark.sql.DataFrame, col_list):
    for c in col_list:
        df_trunc = df.withColumn(c, F.trim(F.col(c)))
    return df_trunc


# COMMAND ----------

def test_trunc(input_df : pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    col_list = ['id']
    filtered_df = remove_spl_char_col(input_df, col_list)
    return filtered_df

# COMMAND ----------

filter_df = test_trunc(df_regex)
display(filter_df)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data2 = [("James","","Smith&","36 636","M",3000, "2000/01/01"),
    ("Michael","Rose","","402 88","M",4000, "1999/06/23"),
    ("Robert","","Williams¬","42 114","M",None, "2001/07/04" ),
    ("Maria","Anne","Jones","39192","F",4000, "1995/08/07"),
    ("Jen","Mary","Brown","","F",-1, "1996/09/08")
  ]

schema = StructType([ \
    StructField("firstName",StringType(),True), \
    StructField("middleName",StringType(),True), \
    StructField("lastName",StringType(),True), \
    StructField("ID", StringType(), True), \
    StructField("Gender", StringType(), True), \
    StructField("Salary", IntegerType(), True), 
    StructField("Date_OF-Birth", StringType(), True)
  ])
 
df_date = spark.createDataFrame(data=data2,schema=schema)
df_date.printSchema()
df_date.show(truncate=False)

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

def test_date(input_df : pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    col_list = ['date_of_birth']
    filtered_df = cast_date_cols(input_df, col_list, "yyyy/mm/dd")
    return filtered_df

# COMMAND ----------

filter_df = test_date(df_date)
display(filter_df)

# COMMAND ----------

import re
def standardise_column_names(input_df: pyspark.sql.DataFrame)->pyspark.sql.DataFrame:
    """
    Parquet and Delta files do not support space in column names and treat as a single column
    this functions substitues the special characters
    
    Args:
        input_df:pyspark.sql.DataFrame
    Returns: pyspark.sql.DataFrame
    """
    
    def clean_spark_col_name(column_name):
        column = column_name
        column = str.replace(column, '+', '_') #Replaces + with _
        column = re.sub('([A-Z]+)', r'_\1', column) #replaces ShiftNo with shift_no and RLN to rln
        column = re.sub(r'[-",;.\{\}(\)\n\t=\s/]', '_', column)
        column = re.sub(r'[#]', '_num_', column)
        column = re.sub(r'[%]', '_perc_', column)
        column = re.sub(r'[\?]', '', column)
        column = re.sub(r'(^_)', '', column)
        column = re.sub(r'(__*)', '_', column)
        column = re.sub(r'(_$)', '', column)
        column = re.sub(r'[[]]', '', column)
        column = re.sub(r'(^_)', '', column)
        return column.lower()
    
    column_remapping = [(original_column, clean_spark_col_name(original_column)) for original_column in
                      input_df.columns]
    output_df = input_df
    for original_column_name, new_column_name in column_remapping:
        output_df = output_df.withColumnRenamed(
            original_column_name, new_column_name)
        
    return output_df

# COMMAND ----------

def test_col_name(input_df : pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
   
    filtered_df = standardise_column_names(input_df)
    return filtered_df

# COMMAND ----------

filter_df = test_col_name(df_date)
display(filter_df)

# COMMAND ----------

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
df_stem.printSchema()
df_stem.show(truncate=False)

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

inp_dict = {'www.intentHq.com' : 'intenthq.com',
             'www.intenthq.com' : 'intenthq.com',
             'https://www.intenthq.com' : 'intenthq.com',
             'intentHQ' : 'intenthq.com'}

col_names_list =['column1', 'column2']


stemmed = convert_dict_to_df_stem(inp_dict, col_names_list, df_stem)
display(stemmed)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data2 = [("James",100 , "2020-03-18 14:41:55"),
        ("Michael", 200, "2020-03-18 14:45:55"),
        ("Robert",300, "2020-03-18 14:51:55"),
        ("Maria", 400, "2020-03-18 15:41:55"),
        ("Jen", 500, "2020-03-18 16:41:55")
  ]

schema = StructType([ \
            StructField("name",StringType(),True), \
            StructField("kg_units",IntegerType(),True), \
            StructField("time_unit",StringType(),True)
                    ])
 
df_hr_agg = spark.createDataFrame(data=data2,schema=schema)
display(df_hr_agg)

# COMMAND ----------

from pyspark.sql.functions import date_format
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


df1 = get_day(df_hr_agg, ts_col_list, 'yyyy-MM-dd HH:mm:ss')
display(df1)

# COMMAND ----------

ts_col_list = ['time_unit']

for c in ts_col_list:
        df = df_hr_agg.withColumn(c, F.to_timestamp(F.col(c), 'yyyy-MM-dd HH:mm:ss'))
        year_col = df.withColumn('year', F.year(F.col(c)))
        
        display(year_col)

# COMMAND ----------

def hourly_aggregation(df : pyspark.sql.DataFrame, ts_col_list: list, ts_format: str, col_list:list ) -> pyspark.sql.DataFrame:
    
    if isinstance(ts_format, str):
        format_len = len(ts_format)
        for i in col_list:
            for c in ts_col_list:
                df = df.withColumn(c, F.to_timestamp(F.col(c).substr(0, format_len), ts_format))
    
                hour_col = (df
                        .withColumn('hour_unit', F.hour(F.col(c)))
                        .groupBy('hour_unit').count().select(i, F.col('count').alias('count_of_units'))
                           )
        return hour_col

# COMMAND ----------


# from pyspark.sql.functions import hour

# def hourly_aggregation(df : pyspark.sql.DataFrame, ts_col_list: list, ts_format: str, order_cols:list ) -> pyspark.sql.DataFrame:
    
#     if isinstance(ts_format, str):
#         format_len = len(ts_format)
#         for c in ts_col_list:
#             df = df.withColumn(c, F.to_timestamp(F.col(c).substr(0, format_len), ts_format))
    
#             hour_col = df.withColumn('hour_unit', F.hour(F.col(c)))
            
#             partition = hour_col.select('hour_unit')
            
#             partition_cols = partition.columns
            
#             w= Window.partitionBy(*partition_cols).orderBy([F.sum(col) for col in order_cols])
#             hr_agg_df = (hour_col
#                            .withColumn('row', F.row_number().over(w)).where(F.col('row') =='1')
                     
#                            .drop('row')
#                            )
#             return hr_agg_df


# COMMAND ----------

ts_col_list = ['time_unit']
col_list  = [ 'kg_units']
df_check100 = hourly_aggregation(df_hr_agg, ts_col_list, 'yyyy-MM-dd HH:mm:ss', col_list)
display(df_check100)

# COMMAND ----------

#suppression of smaller groups:

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data2 = [("bread", 800, "2020-03-18" ),
        ("milk", 700, "2020-03-18"  ),
        ("eggs",100, "2020-03-18" ),
        ("bread", 100, "2020-03-19" ),
        ("eggs", 100, "2020-03-19" ),
        ("bread", 400, "2020-03-20"  ),
        ("milk", 500, "2020-03-20" ),
        ("eggs",100, "2020-03-20" ),
        ("bread", 100, "2020-03-21" ),
        ("eggs", 100, "2020-03-21" ),
        ("bread", 100 , "2020-03-22" ),
        ("milk", 100, "2020-03-22" ),
        ("eggs",100, "2020-03-22" ),
        ("bread", 100, "2020-03-23" ),
        ("eggs", 100, "2020-03-23" )
  ]

schema = StructType([ \
            StructField("unit_name",StringType(),True), \
            StructField("unit_sold",IntegerType(),True), \
            StructField("date",StringType(),True)
                    ])
 
df_supp = spark.createDataFrame(data=data2,schema=schema)
display(df_supp)

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


col_list = ['unit_name']
filter_df= suppression_of_smaller_groups(df_supp, col_list)
display(filter_df)

# COMMAND ----------


