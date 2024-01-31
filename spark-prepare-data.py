# try accessing parquet files using Spark
# source venv/bin/activate

import setuptools
import pandas as pd
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, concat, to_json, substring, udf, pandas_udf
from pyspark.sql.types import StringType

# Set up Spark with extra ram
spark = (
   SparkSession.builder.appName("CompassIoT")
   .config("spark.sql.repl.eagerEval.enabled", True)
   .config("spark.sql.execution.arrow.pyspark.enabled", True)
   .config("spark.sql.parquet.cacheMetadata", "true")
   .config("spark.executor.memory", "8g")
   .config("spark.driver.memory", "8g")
   .config("spark.sql.session.timeZone", "Etc/UTC")
   .getOrCreate()
)

# initialize spark dataframe
folder = './parquet-data'
df = spark.read.parquet(f"{folder}/original")
df.printSchema()

df2 = df.withColumn('start_date', substring("StartTime",1,10))
# df2 = df2.select(['start_date'])
df2.printSchema()

df2.write.parquet(f"{folder}/computed")

# big_df = spark.read.option('mergeSchema','true').parquet(folder)
# big_df.printSchema()
