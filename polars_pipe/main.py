import polars as pl

config = {
    "columns" : [],
    "numeric_columns" : [],
    "categorical_columns" : [],
    "na_drop_records" : ["fare_amount"], # drop records with na in these columns
    "split_columns" : {'passenger_fullname': ['first_name', 'last_name']},
    "mask_full_columns" : ['driver_credit_card', 'passenger_credit_card'],
    "mask_partial_columns" : ['passenger_phone_number', 'driver_phone_number'],
    "columns_to_drop" : ['passenger_fullname'],
    "columns_to_rename" : {'lpep_pickup_datetime': 'pickup_datetime', 'lpep_dropoff_datetime': 'dropoff_datetime'},
    "compute_columns" : []
}

def load_data(df):
    config["columns"] = df.columns
    return df

def modify_data_types(df):
    df = df.with_columns([
        pl.col('VendorID').cast(pl.Int32),
        pl.col('lpep_pickup_datetime').str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f", strict=False),
        pl.col('lpep_dropoff_datetime').str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f", strict=False),
        pl.col('request_datetime').str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f", strict=False),
        pl.col('passenger_count').cast(pl.Int32),
        pl.col('trip_distance').cast(pl.Float64).round(2),
        pl.col('RatecodeID').cast(pl.Int32),
        pl.col('store_and_fwd_flag').cast(pl.Utf8),
        pl.col('PULocationID').cast(pl.Int32),
        pl.col('DOLocationID').cast(pl.Int32),
        pl.col('payment_type').cast(pl.Int32),
        pl.col('fare_amount').cast(pl.Float64),
        pl.col('extra').cast(pl.Float64),
        pl.col('mta_tax').cast(pl.Float64).round(2),
        pl.col('tip_amount').cast(pl.Float64).round(2),
        pl.col('tolls_amount').cast(pl.Float64).round(2),
        pl.col('improvement_surcharge').cast(pl.Float64),
        pl.col('total_amount').cast(pl.Float64).round(2),
        pl.col('congestion_surcharge').cast(pl.Float64),
        pl.col('airport_fee').cast(pl.Float64).round(2),
        pl.col('driver_email').cast(pl.Utf8),
        pl.col('driver_phone_number').cast(pl.Utf8),
        pl.col('driver_fullname').cast(pl.Utf8),
        pl.col('driver_credit_card').cast(pl.Utf8),
        pl.col('passenger_email').cast(pl.Utf8),
        pl.col('passenger_phone_number').cast(pl.Utf8),
        pl.col('passenger_fullname').cast(pl.Utf8),
        pl.col('passenger_credit_card').cast(pl.Utf8),
        pl.col('passenger_address').cast(pl.Utf8),
        pl.col('passenger_Job').cast(pl.Utf8),
        pl.col('passenger_age').cast(pl.Int32),
        pl.col('passenger_sex').cast(pl.Utf8),
        pl.col('pickup_latitude').cast(pl.Float64).round(6),
        pl.col('pickup_longitude').cast(pl.Float64).round(6),
        pl.col('dropoff_latitude').cast(pl.Float64).round(6),
        pl.col('dropoff_longitude').cast(pl.Float64).round(6),
        pl.col('pickup_AQI').cast(pl.Float64),
        pl.col('dropoff_AQI').cast(pl.Float64),
        pl.col('temperature').cast(pl.Float64),
        pl.col('humidity').cast(pl.Float64),
        pl.col('pickup_precipitation_chance').cast(pl.Int32),
        pl.col('uv_index').cast(pl.Float64),
        pl.col('feels_like').cast(pl.Float64),
        pl.col('weather_description').cast(pl.Utf8),
        pl.col('wind_speed_km').cast(pl.Float64)
    ])
    config["numeric_columns"] = df.select(pl.col(pl.Float64, pl.Int32)).columns
    config["categorical_columns"] = df.select(pl.col(pl.Utf8)).columns
    return df

def impute_mean(df):
    for column in config['numeric_columns']:
        mean_value = df[column].mean()
        df = df.with_columns(pl.col(column).fill_null(mean_value))
    return df

def impute_mode(df):
    for column in config['categorical_columns']:
        mode_value = df[column].mode().first()
        df = df.with_columns(pl.col(column).fill_null(mode_value))
    return df

def drop_na(df):
    return df.drop_nulls(subset=config['na_drop_records'])

# todo , not working yet
def split_columns(df):
    for key, column in enumerate(config['split_columns']):
        new_cols = config['split_columns'][column]
        print(f"column: {column}, new_cols: {new_cols}")
        # df = df.with_columns(
        #     pl.col(column).str.split(' ').arr.get(0).alias(new_cols[0]),
        #     pl.col(column).str.split(' ').arr.get(1).alias(new_cols[1])
        # )
        # df = df.with_columns(
        #     pl.col(column).str.split(" ").struct.rename_fields([new_cols[0], new_cols[1]]).unnest(column)
        #     # pl.col(column).str.split(" ").arr.eval(pl.element().arr.get(1)).alias(new_cols[1])
        # )
        df = (df
                .with_columns([
                    pl.lit(column).str.split(" ").alias("names"),
                    pl.col(column).str.split(" ").alias("vals")
                ])
                .explode(["names", "vals"])
                .pivot(index = "x",
                    columns = "names",
                    values = "vals")
        )
    return df

def mask_full_columns(df):
    for column in config['mask_full_columns']:
        pl.lit(11111).alias(column)
    return df

def mask_partial_columns(df):
    for column in config['mask_partial_columns']:
        df = df.with_columns(pl.col(column).apply(lambda x: x[:3] + '***' + x[-3:] if x is not None else x))
    return df

def drop_columns(df):
    return df.drop(config['columns_to_drop'])

def rename_columns(df):
    return df.rename(config['columns_to_rename'])

def compute_columns(df):
    pass

df = pl.read_csv('datasets/output_sample.csv')
df_transformed = (df
                  .pipe(load_data)
                  .pipe(modify_data_types)
                  .pipe(impute_mean)
                  .pipe(impute_mode)
                  .pipe(drop_na)
                #   .pipe(split_columns)
                  .pipe(mask_full_columns)
                #   .pipe(mask_partial_columns)
                #   .pipe(rename_columns)
                #   .pipe(drop_columns)
                  )

print(df_transformed)
df_transformed.write_csv('datasets/output_sample_transformed.csv')
# print(f"numeric columns: {config['numeric_columns']}")
# print(f"categorical columns: {config['categorical_columns']}")