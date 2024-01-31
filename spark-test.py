# try accessing parquet files using Spark
# source venv/bin/activate

import setuptools
import pandas as pd
import json
from flask import Flask, request, jsonify
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, udf
from pyspark.sql.types import StringType
from datetime import datetime, timedelta

# ------------------
# Set up Spark
# spark = SparkSession.builder.getOrCreate()
spark = (
   SparkSession.builder.appName("CompassIoT")
   .config("spark.sql.repl.eagerEval.enabled", True)
   .config("spark.sql.parquet.cacheMetadata", "true")
   .config("spark.executor.memory", "8g")
   .config("spark.driver.memory", "8g")
   .config("spark.sql.session.timeZone", "Etc/UTC")
   .getOrCreate()
)

# initialize spark dataframe
folder = './parquet-data/computed'

# df = spark.read.option('mergeSchema','true').parquet(folder)
df = spark.read.parquet(folder)
df.printSchema()


# ------------------
# Set up Flask API

app = Flask(__name__)

@app.route('/filter', methods=['GET'])
def filter_dataframe():
    # Get query parameters
    vehicle = request.args.get('vehicle')
    trip = request.args.get('trip')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_time')
    first_point = request.args.get('first_point')
    last_point = request.args.get('last_point')
    distance = request.args.get('distance')
    duration = request.args.get('total_time')

    filtered_df = df

    # Apply filters
    if vehicle:
        # VehicleID = '2fc1f51b3e9f5bc382a7440727e5a954'
        filtered_df = filtered_df.filter(filtered_df.VehicleID==vehicle)
    if trip:
        filtered_df = filtered_df.filter(filtered_df.TripID==trip)
    if start_date:
        filtered_df = filtered_df.filter(filtered_df.start_date==start_date)
    if end_date:
        filtered_df = filtered_df.filter(filtered_df.end_date==end_date)

    trimmed = filtered_df.select(['start_date','VehicleID','TripID','Path1'])
    # print('COUNT:', trimmed.count())

    # data = trimmed.collect()
    # print('HELLO', data[0])
    # json_string = json.dumps(data)
    # return json_string

    json = trimmed.toPandas().to_json(orient='records')
    return json

# Start Flask API Server
if __name__ == '__main__':
    app.run(debug=True)



