## todo
# remove duplicates
# outliers


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
    "columns" : [],
    # ['VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'request_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee', 'driver_email', 'driver_phone_number', 'driver_fullname', 'driver_credit_card', 'passenger_email', 'passenger_phone_number', 'passenger_fullname', 'passenger_credit_card', 'passenger_address', 'passenger_Job', 'passenger_age', 'passenger_sex', 'pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude', 'pickup_AQI', 'dropoff_AQI', 'temperature', 'humidity', 'pickup_precipitation_chance', 'uv_index', 'feels_like', 'weather_description', 'wind_speed_km']
    "numeric_columns" : [],
    "categorical_columns" : [],
    "na_drop_records" : ["fare_amount"], # drop records with na in these columns
    "split_columns" : {'passenger_fullname': ['passenger_first_name', 'passenger_last_name'], 'driver_fullname': ['driver_first_name', 'driver_last_name']},
    "mask_full_columns" : ['driver_credit_card', 'passenger_credit_card'],
    "mask_partial_columns" : ['passenger_phone_number', 'driver_phone_number'],
    "columns_to_drop" : ['passenger_fullname', 'driver_fullname'],
    "columns_to_rename" : {'lpep_pickup_datetime': 'pickup_datetime', 'lpep_dropoff_datetime': 'dropoff_datetime'},
    "compute_columns" : [],
    "ordinal_encoding_columns" : ['passenger_sex'],
    "files" : [],
    "s3_path" : "s3a://databricks-workspace-stack-e7e6f-bucket/unity-catalog/taxi-data-dev",
    "processed_path" : "s3a://databricks-workspace-stack-e7e6f-bucket/unity-catalog/taxi-data-dev-processed",
    "target_table" : "app_holozmo_dev.taxitrip_dev_ingest_dev.taxitrips_dlt",
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

# move files using dbutils into new path
def move_files(spark: SparkSession, files: list, new_path: str) -> bool:
    try:
        for file in files:
            dbutils.fs.mv(file, new_path)
            print(f"Moved {file} to {new_path}")
        return True
    except Exception as e:
        print(f"Error in move_files: {e}")
        raise e
    
def load_data(spark: SparkSession) -> DataFrame:
    try:
       # call aws credentials
        aws_credentials = get_aws_credentials(spark)
        # list files
        list_files(spark, config["s3_path"])
        

        # read the data from s3
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(f"{config['s3_path']}/*.csv")
        )

        return df

    except Nothing_To_Load as e:
        print(f"no files to load: {e}")
        raise e
    except Exception as e:
        print(f"Error in load_data: {e}")  
        raise e

def modify_columns_datatypes(df: DataFrame) -> DataFrame:
    try:
      df = (
          df
          .withColumn("VendorID", col("VendorID").cast("integer"))
          .withColumn("lpep_pickup_datetime", col("lpep_pickup_datetime").cast("timestamp"))
          .withColumn("lpep_dropoff_datetime", col("lpep_dropoff_datetime").cast("timestamp"))
          .withColumn("request_datetime", col("request_datetime").cast("timestamp"))
          .withColumn("passenger_count", col("passenger_count").cast("integer"))
          .withColumn("trip_distance", round(col("trip_distance").cast("double"), 2))
          .withColumn("RatecodeID", col("RatecodeID").cast("integer"))
          .withColumn("store_and_fwd_flag", col("store_and_fwd_flag").cast("string"))
          .withColumn("PULocationID", col("PULocationID").cast("integer"))
          .withColumn("DOLocationID", col("DOLocationID").cast("integer"))
          .withColumn("payment_type", col("payment_type").cast("integer"))
          .withColumn("fare_amount", round(col("fare_amount").cast("double"), 2))
          .withColumn("extra", round(col("extra").cast("double"), 1))
          .withColumn("mta_tax", round(col("mta_tax").cast("double"), 2))
          .withColumn("tip_amount", round(col("tip_amount").cast("double"), 2))
          .withColumn("tolls_amount", round(col("tolls_amount").cast("double"), 2))
          .withColumn("improvement_surcharge", round(col("improvement_surcharge").cast("double"), 1))
          .withColumn("total_amount", round(col("total_amount").cast("double"), 1))
          .withColumn("congestion_surcharge", round(col("congestion_surcharge").cast("double"), 1))
          .withColumn("airport_fee", round(col("airport_fee").cast("double"), 2))
          .withColumn("driver_email", col("driver_email").cast("string"))
          .withColumn("driver_phone_number", col("driver_phone_number").cast("string"))
          .withColumn("driver_fullname", col("driver_fullname").cast("string"))
          .withColumn("driver_credit_card", col("driver_credit_card").cast("string"))
          .withColumn("passenger_email", col("passenger_email").cast("string"))
          .withColumn("passenger_phone_number", col("passenger_phone_number").cast("string"))
          .withColumn("passenger_fullname", col("passenger_fullname").cast("string"))
          .withColumn("passenger_credit_card", col("passenger_credit_card").cast("string"))
          .withColumn("passenger_address", col("passenger_address").cast("string"))
          .withColumn("passenger_Job", col("passenger_Job").cast("string"))
          .withColumn("passenger_age", col("passenger_age").cast("integer"))
          .withColumn("passenger_sex", col("passenger_sex").cast("string"))
          .withColumn("pickup_latitude", round(col("pickup_latitude").cast("double"), 6))
          .withColumn("pickup_longitude", round(col("pickup_longitude").cast("double"), 6))
          .withColumn("dropoff_latitude", round(col("dropoff_latitude").cast("double"), 6))
          .withColumn("dropoff_longitude", round(col("dropoff_longitude").cast("double"), 6))
          .withColumn("pickup_AQI", col("pickup_AQI").cast("integer"))
          .withColumn("dropoff_AQI", col("dropoff_AQI").cast("integer"))
          .withColumn("temperature", round(col("temperature").cast("double"), 1))
          .withColumn("humidity", round(col("humidity").cast("double"), 1))
          .withColumn("pickup_precipitation_chance", round(col("pickup_precipitation_chance").cast("double"), 1))
          .withColumn("uv_index", col("uv_index").cast("integer"))
          .withColumn("feels_like", round(col("feels_like").cast("double"), 0))
          .withColumn("weather_description", col("weather_description").cast("string"))
          .withColumn("wind_speed_km", round(col("wind_speed_km").cast("double"), 0))
      )

      # select numeric columns
      config["numeric_columns"] = [field.name for field in df.schema.fields if isinstance(field.dataType, (IntegerType, DoubleType, FloatType))]
      config["categorical_columns"] = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]

      return df
    except Exception as e:
        print(f"Error in modify_columns_datatypes: {e}")
        raise e

def impute_mean(df: DataFrame) -> DataFrame:
    try:
      for column in config['numeric_columns']:
          mean_value = df.selectExpr(f'avg({column}) as mean_value').collect()[0]['mean_value']
          df = df.na.fill({column: mean_value})
      
      return df
    except Exception as e:
        print(f"Error in impute_mean: {e}")
        raise e
    
def impute_mode(df: DataFrame) -> DataFrame:
    try:
      for column in config['categorical_columns']:
          mode_value = df.groupBy(column).count().sort(col("count").desc()).first()[column]
          df = df.na.fill({column: mode_value})
      
      return df
    except Exception as e:
        print(f"Error in impute_mode: {e}")
        raise e
    
def drop_na_for_particular_columns(df: DataFrame) -> DataFrame:
    try:
      for column in config['na_drop_records']:
          df = df.dropna(subset=[column])
      
      return df
    except Exception as e:
        print(f"Error in drop_na_for_particular_columns: {e}")
        raise e
   
def split_columns(df: DataFrame) -> DataFrame:
    try:
      for column, new_columns in config['split_columns'].items():
          df = df.withColumn(new_columns[0], split(col(column), ' ').getItem(0))
          df = df.withColumn(new_columns[1], split(col(column), ' ').getItem(1))
          # df = df.drop(column)
      
      return df
    except Exception as e:
        print(f"Error in split_columns: {e}")
        raise e

def mask_full_columns(df: DataFrame) -> DataFrame:
    try:
      for column in config['mask_full_columns']:
          df = df.withColumn(column, lit("*****"))
      
      return df
    except Exception as e:
        print(f"Error in mask_full_columns: {e}")
        raise e

def mask_partial_columns(df: DataFrame) -> DataFrame:
    try:
        for column in config['mask_partial_columns']:
            df = df.withColumn(column, concat(substring(col(column), 0, 4), lit("***"), substring(col(column), -4, 4)) )
        
        return df
    except Exception as e:
        print(f"Error in mask_partial_columns: {e}")
        raise e  
    
def drop_columns(df: DataFrame) -> DataFrame:
    try:
        for column in config['columns_to_drop']:
            df = df.drop(column)
        
        return df
    except Exception as e:
        print(f"Error in drop_columns: {e}")
        raise e

def rename_columns(df: DataFrame) -> DataFrame:
    try:
        for old_column, new_column in config['columns_to_rename'].items():
            df = df.withColumnRenamed(old_column, new_column)
        
        return df
    except Exception as e:
        print(f"Error in rename_columns: {e}")
        raise e

# encode sex column as Ordinal Encoding
'''
def encode_ordinal(df: DataFrame) -> DataFrame:
    try:
        for column in config['ordinal_encoding_columns']:
            indexer = StringIndexer(inputCol=column, outputCol=f"{column}_index")
            df = indexer.fit(df).transform(df)
            # df = df.drop(column)
            # df = df.withColumnRenamed(f"{column}_index", column)
        
        return df
    except Exception as e:
        print(f"Error in encode_ordinal: {e}")
        raise e
'''
                           
                          

def get_taxis(spark: SparkSession) -> DataFrame:
    try:
        print("Reading taxi data from s3 (get_taxis)...")
        # return ingest.ingest_df(spark)
        # return spark.read.table("samples.nyctaxi.trips")
        
        # read raw files
        df = load_data(spark)

        # pyspark transform
        df = (
            df
            .transform(modify_columns_datatypes)
            .transform(impute_mean)
            .transform(impute_mode)
            .transform(drop_na_for_particular_columns)
            .transform(split_columns)
            .transform(mask_full_columns)
            .transform(mask_partial_columns)
            .transform(drop_columns)
            .transform(rename_columns)
            # .transform(encode_ordinal)
        )

        # add timestamp to the dataframe
        df = df.withColumn("ingest_timestamp", lit(int(time.time())))
        # add inserted datetime with miliseconds
        df = df.withColumn("ingest_datetime", lit(time.strftime('%Y-%m-%d %H:%M:%S:%f')))

        # Set the id column
        df_with_id = df.withColumn("id", monotonically_increasing_id())

       
        # set partition >> TODO

        

        # create and insert data into target table
        df.write.format("delta").mode("append").saveAsTable(config["target_table"])

        # save df as parquet into silver path with filename df_timestamp.parquet
        df.write.format("parquet").mode("append").save(f"{config['s3_path_silver']}/df_{int(time.strftime('%H'))}.parquet")
        df.write.format("csv").mode("overwrite").save(f"{config['s3_path_silver']}/df_{int(time.strftime('%H_%M'))}.csv") # todo will remove
        
        # move files to processed path
        move_files(spark, config["files"], config["processed_path"])

        # display(df)
        return df

    except Nothing_To_Load as e:
        print(f"no files to load: {e}")
        schema = StructType([])
        return spark.createDataFrame([], schema)
    except Exception as e:
        print(f"Error in get_taxis: {e}")
        raise e



# Create a new Databricks Connect session. If this fails,
# check that you have configured Databricks Connect correctly.
# See https://docs.databricks.com/dev-tools/databricks-connect.html.
def get_spark() -> SparkSession:
  try:
    from databricks.connect import DatabricksSession
    return DatabricksSession.builder.getOrCreate()
  except ImportError:
    return SparkSession.builder.getOrCreate()

def main():
  try:
    print(f"Running main >> data transformation...")
    #
    dbutils = globals().get("dbutils", None)
    if dbutils is None:
        raise RuntimeError("dbutils is not available")
    #
    get_taxis(get_spark())#.show(5)

  except Exception as e:
    print(f"Error in main: {e}")
    raise e

if __name__ == '__main__':
  main()
