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
# folder = './sample-data/computed'
folder = './parquet-data/computed'

# df = spark.read.option('mergeSchema','true').parquet(folder)
df = spark.read.parquet(folder)
df.printSchema()


# ------------------
# Set up Flask API

app = Flask(__name__)

@app.route('/path', methods=['GET'])
def get_path():
    # Get query parameters
    trip = request.args.get('trip')
    if not trip: raise RuntimeError('need trip')
    # fetch single trip
    filtered_df = df.filter(df.TripID==trip)
    # trimmed = filtered_df.select(['TripID', 'path', 'Timestamp_path'])
    trimmed = filtered_df.select(['TripID', 'path'])
    # output
    json = trimmed.toPandas().to_json(orient='records')
    return json


@app.route('/filter', methods=['GET'])
def filter_dataframe():
    # Get query parameters
    vehicle = request.args.get('vehicle')
    veh_type = request.args.get('veh_type')
    trip = request.args.get('trip')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_time')
    start_lon = request.args.get('start_lon')
    start_lat = request.args.get('start_lat')
    end_lon = request.args.get('end_lon')
    end_lat = request.args.get('end_lat')
    distance = request.args.get('distance')
    duration = request.args.get('total_time')

    filtered_df = df

    # Apply filters
    if start_date:
        filtered_df = filtered_df.filter(filtered_df.start_date==start_date)
    if end_date:
        filtered_df = filtered_df.filter(filtered_df.end_date==end_date)
    if start_lon:
        filtered_df = filtered_df.filter(filtered_df.start_lon.startswith(start_lon))
    if start_lat:
        filtered_df = filtered_df.filter(filtered_df.start_lat.startswith(start_lat))
    if end_lon:
        filtered_df = filtered_df.filter(filtered_df.start_lon.startswith(end_lon))
    if end_lat:
        filtered_df = filtered_df.filter(filtered_df.start_lat.startswith(end_lat))
    if vehicle:
        filtered_df = filtered_df.filter(filtered_df.VehicleID==vehicle)
    if veh_type:
        filtered_df = filtered_df.filter(filtered_df.veh_types==veh_type)
    if trip:
        filtered_df = filtered_df.filter(filtered_df.TripID==trip)

    # trimmed = filtered_df
    trimmed = filtered_df.select(['VehicleID','TripID','start_date','start_time','end_date','end_time','start_lon','start_lat','end_lon','end_lat','total_time','TravelDistanceMeters'])
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



