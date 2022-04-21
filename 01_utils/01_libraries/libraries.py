# Databricks notebook source
import json
import pyspark.sql
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from datetime import datetime, date, timedelta
from typing import Union
import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from functools import reduce
from pyspark.sql.window import Window
from pyspark.sql.functions import hour
from pyspark.sql.functions import date_format

# COMMAND ----------


