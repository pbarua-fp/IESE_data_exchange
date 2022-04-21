# Databricks notebook source
import boto3
import botocore
import json
from json.decoder import JSONDecodeError

# COMMAND ----------

def get_json_config_from_file(file_path: str) -> dict:
    """
    Retrieves a JSON file from local path and converts to json
    """
    try:
        with open(file_path, "r") as file:
            json_str = file.read()
            config_dict = json.loads(json_str)
    except FileNotFoundError:
        raise ValueError(
            "Error retriving config file from location.  Check location is correct."
        )
    except JSONDecodeError:
        raise ValueError(
            "Unable to parse JSON file.  Please check the file is valid JSON."
        )
    return config_dict

# COMMAND ----------

def check_config_structure(config_dict: dict) -> None:
    """
    Carries out basic checks on the structure and keys in the config file.
    Throws ValueError if error in config is found.
    """
    if "metadata" not in config_dict:
        raise ValueError(
            "The 'metadata' field is not present in the config.  Make sure this mandatory value is present."
        )
    if "tables" not in config_dict:
        raise ValueError(
            "The 'tables' field is not present in the config.  Make sure this mandatory value is present."
        )
    for table in config_dict["tables"]:
        if "tableName" not in table:
            raise ValueError(
                "The 'tableName' field is not present for all tables in the config.  Make sure this mandatory value is present."
            )
        if "columns" not in table:
            raise ValueError(
                f"The 'columns' field is not present in table '{table['tableName']}' in the config.  Make sure this mandatory value is present."
            )
        for column in table["columns"]:
            if "columnName" not in column:
                raise ValueError(
                    f"The 'columnName' field is not present for all columns in table '{table['tableName']}' in the config.  Make sure this mandatory value is present."
                )
            if "operations" not in column:
                raise ValueError(
                    f"The 'operations' field is not present for all columns in table '{table['tableName']}' in the config.  Make sure this mandatory value is present."
                )
            for operation in column["operations"]:
                if "type" not in operation:
                    raise ValueError(
                        f"The 'type' field is not present for all operations in column '{column['columnName']}' of table '{table['tableName']}' in the config.  Make sure this mandatory value is present."
                    )
                if "sequence" not in operation:
                    raise ValueError(
                        f"The 'sequence' field is not present for all operations in column '{column['columnName']}' of table '{table['tableName']}' in the config.  Make sure this mandatory value is present."
                    )


# COMMAND ----------

def get_column_operations(
    config_dict: dict, table_name: str, column_name: str
) -> dict:
    """
    Returns the parameters specified in the config file for a specific operation.
    Most of this function is error checking the JSON.
    """
    check_config_structure(config_dict)

    table = [i for i in config_dict["tables"] if i["tableName"] == table_name]
    if not table:
        raise ValueError(f"Table {table_name} is not present in the config")
    table = table[0]

    column = [i for i in table["columns"] if i["columnName"] == column_name]
    if not column:
        raise ValueError(f"Column '{column_name}' is not present in the config for table '{table_name}'.")
    column = column[0]
    operations_dict = column['operations']
    return operations_dict

