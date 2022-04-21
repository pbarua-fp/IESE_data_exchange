# Databricks notebook source
# MAGIC %md This notebook is to create reusable unit functions that are for the initial reporting.
# MAGIC They comprise of :
# MAGIC 1. columns check
# MAGIC 2. standardising column names
# MAGIC 3. datatype casting ( string, int, decimal)
# MAGIC 4. inferred datatypes
# MAGIC 5. min/max values
# MAGIC 6. null count
# MAGIC 7. count of distinct values (cardinality)
# MAGIC 8. Handling nulls - convert nulls to Decimal/Boolean Values

# COMMAND ----------

# MAGIC %md import libraries

# COMMAND ----------

# MAGIC %run /Shared/iese_data_exchange/01_utils/01_libraries/libraries

# COMMAND ----------

# MAGIC %md #column check

# COMMAND ----------

def check_columns_exist(input_df: pyspark.sql.DataFrame, col_list: list) ->pyspark.sql.DataFrame:
    """
    This function is to check whether a column exist or doesnt in the input dataframe
    """
    listColumns = input_df.columns
    for c in col_list:
        if c not in listColumns:
            print(c +'not in source dataframe')
            return False
        else:
            print(c + 'is in source dataframe')
            return True
             

# COMMAND ----------

# MAGIC %md #Standarising column names

# COMMAND ----------

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

# MAGIC %md # string datatype casting

# COMMAND ----------

def cast_string_cols(df : pyspark.sql.DataFrame, col_list):
    """
    cast multiple columns in string type into its string type.
    """
    for c in col_list:
        df = df.withColumn(c, (F.col(c).cast(StringType())))
               
    return df

# COMMAND ----------

# MAGIC %md # int datatype casting

# COMMAND ----------

def cast_int_cols(df : pyspark.sql.DataFrame, col_list):
    """
    cast multiple columns in string type into its integer type.
    """
    for c in col_list:
        df = df.withColumn(c, (F.col(c).cast(IntegerType())))
               
    return df

# COMMAND ----------

# MAGIC %md #float datatype casting

# COMMAND ----------

def cast_float_cols(df : pyspark.sql.DataFrame, col_list):
    """
    cast multiple columns in string type into its decimal type.
    """
    for c in col_list:
        df = df.withColumn(c, (F.col(c).cast(DecimalType())))
               
    return df

# COMMAND ----------

# MAGIC %md #min value for a column

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

# MAGIC %md #max value for a column

# COMMAND ----------

def max_value(df : pyspark.sql.DataFrame, col_list, group_col_list) -> pyspark.sql.DataFrame:
    """
    get maximum value for a group of columns.
    """
    for c in col_list:
        for i in group_col_list:
            df = (df
                  .groupBy(F.col(i))
                  .agg({c: 'max'})
                 )
               
    return df

# COMMAND ----------

# MAGIC %md #count for a column

# COMMAND ----------

def count_value(df : pyspark.sql.DataFrame, col_list, group_col_list) -> pyspark.sql.DataFrame:
    """
    get count for a group of columns.
    """
    for c in col_list:
        for i in group_col_list:
            df = (df
                  .groupBy(F.col(i))
                  .agg({c: 'count'})
                 )
               
    return df

# COMMAND ----------

# MAGIC %md #avg value of a column

# COMMAND ----------

def avg_value(df : pyspark.sql.DataFrame, col_list, group_col_list) -> pyspark.sql.DataFrame:
    """
    get avg value for a group of columns.
    """
    for c in col_list:
        for i in group_col_list:
            df = (df
                  .groupBy(F.col(i))
                  .agg({c: 'avg'})
                 )
               
    return df

# COMMAND ----------

# MAGIC %md # get max value / latest value for partitioned data

# COMMAND ----------

def get_max_rows( unfiltered_df : pyspark.sql.DataFrame,
                              partition_cols : list,
                              order_cols: list) -> pyspark.sql.DataFrame:
    """
    gets the rows using window function to get latest or of highest value.
    """
    w= Window.partitionBy(*partition_cols).orderBy([F.desc_nulls_last(col) for col in order_cols])
    filtered_df = (unfiltered_df
                   .withColumn('row', F.row_number().over(w)).where(F.col('row') =='1')
                   .drop('row')
                  )
    
    return filtered_df

# COMMAND ----------

# MAGIC %md #get min value/ for partitioned data

# COMMAND ----------

def get_min_rows( unfiltered_df : pyspark.sql.DataFrame,
                              partition_cols : list,
                              order_cols: list) -> pyspark.sql.DataFrame:
    """
    gets the rows using window function to get latest or of highest value.
    """
    w= Window.partitionBy(*partition_cols).orderBy([F.asc(col) for col in order_cols])
    filtered_df = (unfiltered_df
                   .withColumn('row', F.row_number().over(w)).where(F.col('row') =='1')
                   .drop('row')
                  )
    
    return filtered_df

# COMMAND ----------

# MAGIC %md #get avg value/ for partitioned data

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

# MAGIC %md # cardinality for a group of columns

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

# MAGIC %md # convert null to double

# COMMAND ----------

def convert_null_to_double(input_df : pyspark.sql.DataFrame, col_list: list) -> pyspark.sql.DataFrame:
    """
    casts the null value of a column to a double value for a string datatype or cast as Double.
    """
    
    
    for c in col_list:
        df1 = df.withColumn(c, F.when(F.col(c).cast('double').isNull(), F.lit(0.0))
        .otherwise(F.col(c).cast('double')))
    return df1

# COMMAND ----------

# MAGIC %md # Check the count of null values

# COMMAND ----------

def null_count_col_list(df: pyspark.sql.DataFrame, col_list: list) -> list:
    """
    To give the count of the null values.
    """
    for c in col_list:
        null_count = (df
                      .filter(F.col(c).isNull())
                     ).count()
    
    return null_count

# COMMAND ----------

# MAGIC %md # inferred datatypes as per dictionary

# COMMAND ----------

def add_missing_cols_with_types(df:pyspark.sql.DataFrame, col_name_type: dict) -> pyspark.sql.DataFrame:
    """
    check if the col is in the df and then add it to the schema if its not
    :param pyspark.sql.DataFrame df:dataset
    :param dict col_name_type: col_name and col_type
    :return pyspark.sql.DataFrame: dataset with the newly added column and its type
    """
    for col_name, col_type in col_name_type.items():
        if col_name not in df.columns:
            df = (df
                  .withColumn(col_name, F.lit(None))
                  .withColumn(col_name, F.col(col_name).cast(col_type))
                 )
    
    return df
