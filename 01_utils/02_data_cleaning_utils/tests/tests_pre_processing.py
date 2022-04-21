# Databricks notebook source
# MAGIC %md ## Unit tests for the pre-processing unit functions

# COMMAND ----------

# MAGIC %run ../pre_processing_unit_functions

# COMMAND ----------

import hashlib

# COMMAND ----------

# MAGIC %md ## Hashing with salt

# COMMAND ----------

def _generate_dataframe():
    data = [
        ("Paul", "AB356JU", 5),
        ("Priyanka", "BL13TG", 10),  # obviously these aren't our real postcodes ;)
    ]
    df = spark.createDataFrame(data=data, schema=['name','postcode', 'number_of_horses_owned'])
    return df

# COMMAND ----------

salt = 'dfjn5jn435nn324lk23m4lkm23lk4m2lk34j23k4jnjngm54'

def test_hash_single_column_no_bit_length():
    df = hash_column_with_salt(df=_generate_dataframe(), column_name='name', salt_value=salt)
    hs = hashlib.sha256(f'Paul{salt}'.encode('utf-8')).hexdigest()
    assert df.first()['name'] == hs
    assert df.first()['postcode'] == 'AB356JU'
    assert df.filter(F.col('name').rlike("^[a-f0-9]{64}$")).count() == 2

def test_hash_single_column_with_bit_length():
    df = hash_column_with_salt(df=_generate_dataframe(), column_name='name', salt_value=salt, sha_length=256)
    hs = hashlib.sha256(f'Paul{salt}'.encode('utf-8')).hexdigest()
    assert df.first()['name'] == hs
    assert df.first()['postcode'] == 'AB356JU'
    

def test_hash_single_column_with_nulls():
    data = [
        (None, "AB356JU", 5),
        ("Priyanka", "BL13TG", 10),  # obviously these aren't our real postcodes ;)
    ]
    df = spark.createDataFrame(data=data, schema=['name','postcode', 'number_of_horses_owned'])
    df = hash_column_with_salt(df=df, column_name='name', salt_value=salt, sha_length=256)
    assert df.first()['name'] == None
    assert df.first()['postcode'] == 'AB356JU'
    

def test_hash_number_column():
    df = hash_column_with_salt(df=_generate_dataframe(), column_name='number_of_horses_owned', salt_value=salt, sha_length=256)
    hs = hashlib.sha256(f'5{salt}'.encode('utf-8')).hexdigest()
    assert df.first()['name'] == 'Paul'
    assert df.first()['postcode'] == 'AB356JU'
    assert df.first()['number_of_horses_owned'] == hs 
    assert df.filter(F.col('number_of_horses_owned').rlike("^[a-f0-9]{64}$")).count() == 2
    
def test_bad_salt_value():
    try:
        df = hash_column_with_salt(df=_generate_dataframe(), column_name='number_of_horses_owned', salt_value=salt, sha_length=259)
        assert False
    except ValueError:
        pass
    

test_hash_single_column_no_bit_length()
test_hash_single_column_with_bit_length()
test_hash_single_column_with_nulls()
test_hash_number_column()
test_bad_salt_value()
print('All tests passed')
