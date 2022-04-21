# Databricks notebook source
# MAGIC %md ## Tests for the JSON utils

# COMMAND ----------

import boto3
import json


# COMMAND ----------

# MAGIC %run ../02_json_config_utils

# COMMAND ----------

# MAGIC %md ### Setup and Example Config 

# COMMAND ----------

example_json = """{
    "metadata": {
        "createdBy": "Paul Symmers"
    },
    "tables": [
        {
            "tableName": "person",
            "columns": [
                {
                    "columnName": "first_name",
                    "operations": [
                        {
                            "type": "hash",
                            "sequence": 1,
                            "parameters": {
                                "bitLength": 256
                            }
                        }
                    ]
                },
                {
                    "columnName": "surname",
                    "operations": [
                        {
                            "type": "hash",
                            "sequence": 2,
                            "parameters": {
                                "bitLength": 256
                            }
                        }
                    ]
                }
            ]
        },
        {
            "tableName": "call",
            "columns": [
                {
                    "columnName": "call_id",
                    "operations": [
                        {
                            "type": "hash",
                            "sequence": 1,
                            "parameters": {
                                "bitLength": 256
                            }
                        }
                    ]
                },
                {
                    "columnName": "call_duration",
                    "operations": [
                        {
                            "type": "hash",
                            "sequence": 2,
                            "parameters": {
                                "bitLength": 256
                            }
                        }
                    ]
                }
            ]
        }
    ]
}
"""

dbutils.fs.rm('/tmp/example_json.json')
dbutils.fs.put('/tmp/example_json.json', example_json)
bad_json = example_json[:10] + '"' + example_json[10:]
dbutils.fs.rm('/tmp/bad_json.json')
dbutils.fs.put('/tmp/bad_json.json', bad_json)

# COMMAND ----------

# MAGIC %md ### Unit tests

# COMMAND ----------


def test_load_from_file():
    returned_dict = get_json_config_from_file("/dbfs/tmp/example_json.json")
    assert type(returned_dict) == dict
    assert "tables" in returned_dict
    assert returned_dict["tables"][0]["columns"][0]["operations"][0]["type"] == "hash"
    


def test_load_from_file_bad_location():
    try:
        returned_dict = get_json_config_from_file("/dbfs/t_json.json")
    except ValueError:
        pass
    

def test_bad_json():
    try:
        returned_dict = get_json_config_from_file("/tmp/bad_json.json")
        assert False == True
    except ValueError:
        pass


def config_example():
    with open("/dbfs/tmp/example_json.json", "r") as file:
        config = json.loads(file.read())
    return config


def test_config_checking_good(config_example):
    check_config_structure(config_dict=config_example)


def test_config_checking_missing_tables(config_example):
    del config_example["tables"]
    try:
        check_config_structure(config_dict=config_example)
        assert False == True
    except ValueError:
        pass
    

def test_get_column_operations(config):
    ops = get_column_operations(config_dict=config, table_name='person', column_name='surname')
    assert ops[0]['type'] == 'hash'
    assert ops[0]['sequence'] == 2


def test_get_column_operations_no_table(config):
    try:
        ops = get_column_operations(config_dict=config, table_name='peron', column_name='surname')
        assert True == False
    except ValueError:
        pass


def test_get_column_operations_no_column(config):
    try:
        ops = get_column_operations(config_dict=config, table_name='person', column_name='surnae')
        assert True == False
    except ValueError:
        pass
    


test_load_from_file()
test_load_from_file_bad_location()
test_bad_json()
test_config_checking_good(config_example())
test_config_checking_missing_tables(config_example())
test_get_column_operations(config_example())
test_get_column_operations_no_table(config_example())
test_get_column_operations_no_column(config_example())
