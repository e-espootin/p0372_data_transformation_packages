import time
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, substring, concat, split, round, col, monotonically_increasing_id
from pyspark.sql.types import IntegerType, DoubleType, FloatType, StringType,StructType 
# from pyspark.ml.feature import StringIndexer
from databricks.sdk.runtime import *
import sys
import os
from pathlib import Path


# Get the current working directory
current_dir = os.getcwd()
print(f"current path {current_dir}")

# Define the relative path to your package
package_relative_path = "ingest_data"

# Construct the full path to the package
# package_root = os.path.join(current_dir, package_relative_path)
package_root = current_dir
print(f"package root {package_root}")

# Add the package root to sys.path
sys.path.append(package_root)

# todo later
# from ingest_data import ingest

class Nothing_To_Load(Exception):
    pass

config = {
    "columns" : ['VendorID',
        'pickup_datetime',
        'dropoff_datetime',
        'request_datetime',
        'passenger_count',
        'trip_distance',
        'RatecodeID',
        'store_and_fwd_flag',
        'PULocationID',
        'DOLocationID',
        'payment_type',
        'fare_amount',
        'extra',
        'mta_tax',
        'tip_amount',
        'tolls_amount',
        'improvement_surcharge',
        'total_amount',
        'congestion_surcharge',
        'airport_fee',
        'driver_email',
        'driver_phone_number',
        'driver_credit_card',
        'passenger_email',
        'passenger_phone_number',
        'passenger_credit_card',
        'passenger_address',
        'passenger_Job',
        'passenger_age',
        'passenger_sex',
        'pickup_latitude',
        'pickup_longitude',
        'dropoff_latitude',
        'dropoff_longitude',
        'pickup_AQI',
        'dropoff_AQI',
        'temperature',
        'humidity',
        'pickup_precipitation_chance',
        'uv_index',
        'feels_like',
        'weather_description',
        'wind_speed_km',
        'passenger_first_name',
        'passenger_last_name',
        'driver_first_name',
        'driver_last_name'],
    "source_table" : "app_holozmo_dev.taxitrip_dev_ingest_dev.taxitrips_dlt",
    "target_table" : "app_holozmo_dev.taxitrip_dev_ingest_dev.taxitrips_dlt_gold",
    "s3_path_gold" : "s3://databricks-workspace-stack-e7e6f-bucket/unity-catalog/taxi_data_dev_gold",
    "s3_path_silver" : "s3://databricks-workspace-stack-e7e6f-bucket/unity-catalog/taxi_data_dev_silver",
}

# def Retrieve the AWS credentials from the secret scope
def get_aws_credentials(spark: SparkSession) -> dict:
    try:
        access_key = dbutils.secrets.get(scope="aws-scope", key="aws-access-key")
        secret_key = dbutils.secrets.get(scope="aws-scope", key="aws-secret-key")

        # set the credentials
        spark.conf.set("fs.s3a.access.key", access_key)
        spark.conf.set("fs.s3a.secret.key", secret_key)

        return {"access_key": access_key, "secret_key": secret_key}
    except Exception as e:
      print(f"Error in get_aws_credentials: {e}")
      raise e
    
def list_files(spark: SparkSession, path: str) -> list:
    try:
        files = dbutils.fs.ls(path)
        print(f"Files in {path}: {files}")

        config["files"] = [file.path for file in files]
        
        if len(config["files"]) == 0:
            raise Nothing_To_Load("No files found in the path")
        
        return True
    except Exception as e:
        print(f"Error in list_files: {e}")
        raise e

# get last processed timestamp
def get_last_processed_timestamp(spark: SparkSession) -> int:
    try:
        # read last timestamp from target table
        df = (
            spark.read.format("delta")
            .table(config["target_table"])
            .orderBy(col("ingest_timestamp").desc())
            .limit(1)
        )

        # get the last processed timestamp
        last_processed_timestamp = df.select("ingest_timestamp").collect()[0][0]
        print(f"Last processed timestamp: {last_processed_timestamp}")

        return last_processed_timestamp
    except Exception as e:
        print(f"Error in get_last_processed_timestamp: {e}")
        # raise e
    
def load_data(spark: SparkSession, last_processed_timestamp: int) -> DataFrame:
    try:
       # call aws credentials
        aws_credentials = get_aws_credentials(spark)
        # list files
        # list_files(spark, config["s3_path_silver"])
        

        # read the data from s3
        # df = (
        #     spark.read.format("csv")
        #     .option("header", "true")
        #     .option("inferSchema", "true")
        #     .load(f"{config['s3_path']}/*.csv")
        # )

        # read data from delta table with filter timestamp > last_processed_timestamp
        if last_processed_timestamp:
            df = spark.read.format("delta").table(config["source_table"]).filter(col("ingest_timestamp") > last_processed_timestamp)
        else:
            df = spark.read.format("delta").table(config["source_table"])

        print(f"Data loaded: {df.count()} rows")
        return df

    except Nothing_To_Load as e:
        print(f"no files to load: {e}")
        raise e
    except Exception as e:
        print(f"Error in load_data: {e}")  
        raise e

def pickup_hour(df: DataFrame) -> DataFrame:
    try:
        df = df.withColumn("pickup_hour", substring("pickup_datetime", 12, 2))
        return df
    except Exception as e:
        print(f"Error in pickup_hour: {e}")
        raise e
    
# pickup day of week >> pickup_day_of_week
def pickup_day_of_week(df: DataFrame) -> DataFrame:
    try:
        df = df.withColumn("pickup_day_of_week", substring("pickup_datetime", 9, 2))
        # convert day to weekday
        df = df.withColumn("pickup_day_of_week", 
                           concat(
                               lit("0"), 
                               round((col("pickup_day_of_week").cast(IntegerType()) - 1) % 7).cast(StringType())
                           )
                          )
        return df
    except Exception as e:
        print(f"Error in pickup_day_of_week: {e}")
        raise e
    
# trip duration minute >> trip_duration
def trip_duration(df: DataFrame) -> DataFrame:
    try:
        df = df.withColumn("trip_duration_minute", 
                           (col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")) / 60
                          )
        return df
    except Exception as e:
        print(f"Error in trip_duration: {e}")
        raise e
    
# Request-to-Pickup Delay
def request_to_pickup_delay(df: DataFrame) -> DataFrame:
    try:
        df = df.withColumn("request_to_pickup_delay_min", 
                           (col("pickup_datetime").cast("long") - col("request_datetime").cast("long")) / 60
                          )
        return df
    except Exception as e:
        print(f"Error in request_to_pickup_delay: {e}")
        raise e

# trip distance in km
def trip_distance_km(df: DataFrame) -> DataFrame:
    try:
        df = df.withColumn("trip_distance_km", round(col("trip_distance") * 1.60934,2))
        return df
    except Exception as e:
        print(f"Error in trip_distance: {e}")
        raise e

# Total Tip Percentage
def total_tip_percentage(df: DataFrame) -> DataFrame:
    try:
        df = df.withColumn("total_tip_percentage", round(col("tip_amount") / col("total_amount") * 100,1))
        return df
    except Exception as e:
        print(f"Error in total_tip_percentage: {e}")
        raise e

# Fare per Distance _ miles
def fare_per_distance(df: DataFrame) -> DataFrame:
    try:
        df = df.withColumn("fare_per_distance", round(col("fare_amount") / col("trip_distance"),2))
        return df
    except Exception as e:
        print(f"Error in fare_per_distance: {e}")
        raise e
    
# Revenue per Minute
def revenue_per_minute(df: DataFrame) -> DataFrame:
    try:
        df = df.withColumn("revenue_per_minute", round(col("total_amount") / col("trip_duration_minute"),2))
        return df
    except Exception as e:
        print(f"Error in revenue_per_minute: {e}")
        raise e

# Day/Night Indicator with considering the sunrise and sunset of seasons
def is_daylight(df: DataFrame) -> DataFrame:
    try:
        # get month
        df = df.withColumn("month", substring("pickup_datetime", 6, 2))
        # if month is between 4 and 9, it is summer and hour is between 6 and 20, it is day 6 and 20 then day , else night
        
        df = df.withColumn("is_daylight", 
                           ( col("month").cast(IntegerType()).between(4, 9) & col("pickup_hour").cast(IntegerType()).between(6, 20))
                          )
        
        return df
    except Exception as e:
        print(f"Error in day_night_indicator: {e}")
        raise e

# is rush hour
def is_rush_hour(df: DataFrame) -> DataFrame:
    try:
        # if hour is between 7 and 9 or between 16 and 18, it is rush hour
        df = df.withColumn("is_rush_hour", 
                           ( col("pickup_hour").cast(IntegerType()).between(7, 9) | col("pickup_hour").cast(IntegerType()).between(16, 18))
                          )
        
        return df
    except Exception as e:
        print(f"Error in is_rush_hour: {e}")
        raise e

# average speed >> trip_distance / (trip_duration_minutes / 60
def average_speed(df: DataFrame) -> DataFrame:
    try:
        df = df.withColumn("average_speed", round(col("trip_distance") / (col("trip_duration_minute") / 60), 0))
        return df
    except Exception as e:
        print(f"Error in average_speed: {e}")
        raise

# calculate busy area with PULocationID , DOLocationID
def busy_area(df: DataFrame) -> DataFrame:
    try:
        # group by PULocationID and DOLocationID and return count
        df_agg = df.groupBy("PULocationID", "DOLocationID").count()
        # todo
        return df
    except Exception as e:
        print(f"Error in busy_area: {e}")
        raise

# route >> pickup_zip, '-', dropoff_zip
def route(df: DataFrame) -> DataFrame:
    try:
        df = df.withColumn("route", concat(col("PULocationID"), lit("-"), col("DOLocationID")))
        return df
    except Exception as e:
        print(f"Error in route: {e}")
        raise

################## Aggregated Features ##################

# Passenger Loyalty , aggregate the number of trips for each passenger , filter with email of passenger
# agg
def passenger_loyalty(df: DataFrame) -> DataFrame:
    try:
        # agg with email and return count
        df_agg = df.groupBy("passenger_email").count()
        # add column passenger_loyalty to df as loyalty
        df = df.join(df_agg, "passenger_email", "left").withColumnRenamed("count", "passenger_loyalty")

        return df
    except Exception as e:
        print(f"Error in passenger_loyalty: {e}")
        raise e

# Driver Experience Level , aggregate the number of trips for each driver 
# agg
def driver_experience_level(df: DataFrame) -> DataFrame:
    try:
        # agg with driver_id and return count
        df_agg = df.groupBy("driver_email").count()
        # add column driver_experience_level to df as experience_level
        df = df.join(df_agg, "driver_email", "left").withColumnRenamed("count", "driver_experience_level")

        return df
    except Exception as e:
        print(f"Error in driver_experience_level: {e}")
        raise e                       

def transform_silver_data(spark: SparkSession) -> DataFrame:
    try:
        print("Reading silver data (transform_data)...")

        # get last processed timestamp
        last_processed_timestamp = get_last_processed_timestamp(spark)

        # read raw files
        df = load_data(spark, last_processed_timestamp)

        
        # pyspark transform
        df = (
            df
            .transform(pickup_hour)
            .transform(pickup_day_of_week)
            .transform(trip_duration)
            .transform(request_to_pickup_delay)
            .transform(trip_distance_km)
            .transform(total_tip_percentage)
            .transform(fare_per_distance)
            .transform(revenue_per_minute)
            .transform(is_daylight)
            .transform(is_rush_hour)
            .transform(average_speed)
            ## .transform(busy_area)
            .transform(route)
            .transform(passenger_loyalty)
            .transform(driver_experience_level)
        )

        # create and insert data into target table
        df.write.format("delta").mode("append").saveAsTable(config["target_table"])

        # save df as parquet into silver path with filename df_timestamp.parquet
        df.write.format("parquet").mode("append").save(f"{config['s3_path_gold']}/df_gold_layer.parquet")

        # display(df)
        return df
    except Nothing_To_Load as e:
        print(f"no files to load: {e}")
        schema = StructType([])
        return spark.createDataFrame([], schema)
    except Exception as e:
        print(f"Error in get_taxis: {e}")
        raise e
   
def get_spark() -> SparkSession:
  try:
    from databricks.connect import DatabricksSession
    return DatabricksSession.builder.getOrCreate()
  except ImportError:
    return SparkSession.builder.getOrCreate()
  
def main():
    try:
        print(f"Running main >> data transformation...")
        transform_silver_data(get_spark())
    
    except Exception as e:
        print(f"Error in main: {e}")
        raise e  

if __name__ == '__main__':
    main()  