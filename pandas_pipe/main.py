import pandas as pd

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
    config["columns"] = df.columns.tolist()
    return df

def modify_data_types(df):
    df['VendorID'] = df['VendorID'].astype(int)
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    df['request_datetime'] = pd.to_datetime(df['request_datetime'])
    df['passenger_count'] = df['passenger_count'].astype(int)
    df['trip_distance'] = df['trip_distance'].astype(float).round(2)
    df['RatecodeID'] = df['RatecodeID'].astype(int)
    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].astype(str)
    df['PULocationID'] = df['PULocationID'].astype(int)
    df['DOLocationID'] = df['DOLocationID'].astype(int)
    df['payment_type'] = df['payment_type'].astype(int)
    df['fare_amount'] = df['fare_amount'].astype(float)
    df['extra'] = df['extra'].astype(float)
    df['mta_tax'] = df['mta_tax'].astype(float).round(2)
    df['tip_amount'] = df['tip_amount'].astype(float).round(2)
    df['tolls_amount'] = df['tolls_amount'].astype(float).round(2)
    df['improvement_surcharge'] = df['improvement_surcharge'].astype(float)
    df['total_amount'] = df['total_amount'].astype(float).round(2)
    df['congestion_surcharge'] = df['congestion_surcharge'].astype(float)
    df['airport_fee'] = df['airport_fee'].astype(float).round(2)
    df['driver_email'] = df['driver_email'].astype(str)
    df['driver_phone_number'] = df['driver_phone_number'].astype(str)
    df['driver_fullname'] = df['driver_fullname'].astype(str)
    df['driver_credit_card'] = df['driver_credit_card'].astype(str)
    df['passenger_email'] = df['passenger_email'].astype(str)
    df['passenger_phone_number'] = df['passenger_phone_number'].astype(str)
    df['passenger_fullname'] = df['passenger_fullname'].astype(str)
    df['passenger_credit_card'] = df['passenger_credit_card'].astype(str)
    df['passenger_address'] = df['passenger_address'].astype(str)
    df['passenger_Job'] = df['passenger_Job'].astype(str)
    df['passenger_age'] = df['passenger_age'].astype(int)
    df['passenger_sex'] = df['passenger_sex'].astype(str)
    df['pickup_latitude'] = df['pickup_latitude'].astype(float).round(6)
    df['pickup_longitude'] = df['pickup_longitude'].astype(float).round(6)
    df['dropoff_latitude'] = df['dropoff_latitude'].astype(float).round(6)
    df['dropoff_longitude'] = df['dropoff_longitude'].astype(float).round(6)
    df['pickup_AQI'] = df['pickup_AQI'].astype(float)
    df['dropoff_AQI'] = df['dropoff_AQI'].astype(float)
    df['temperature'] = df['temperature'].astype(float)
    df['humidity'] = df['humidity'].astype(float)
    df['pickup_precipitation_chance'] = df['pickup_precipitation_chance'].astype(int)
    df['uv_index'] = df['uv_index'].astype(float)
    df['feels_like'] = df['feels_like'].astype(float)
    df['weather_description'] = df['weather_description'].astype(str)
    df['wind_speed_km'] = df['wind_speed_km'].astype(float)
    #
    config["numeric_columns"] = df.select_dtypes(include=['number', 'int64', 'float64']).columns.tolist()
    config["categorical_columns"]  = df.select_dtypes(include=['object']).columns.tolist()
    #
    return df

def impute_mean(df):
    for column in config['numeric_columns']:
        df[column].fillna(df[column].mean(), inplace=True)
        # todo rewrite map
        # df[column] = df[column].map(lambda x: x.fillna(x.mean()))
    return df

def impute_mode(df):
    for column in config['categorical_columns']:
        df[column].fillna(df[column].mode()[0], inplace=True)
    return df

# drop na with filter
def drop_na(df):
    return df.dropna(subset=config['na_drop_records'])

# split column fullname
def split_columns(df):
    for key, column in enumerate(config['split_columns']):
        new_cols = config['split_columns'][column]
        # print(f"new_cols: {new_cols}")
        df[new_cols[0]] = df[column].str.split(' ').str[0]
        df[new_cols[1]] = df[column].str.split(' ').str[1]
    return df

# mask full columns
def mask_full_columns(df):
    for column in config['mask_full_columns']:
        df[column] = df[column].mask(df[column].notnull(), '*****')
    return df

# mask partial columns
def mask_partial_columns(df):
    for column in config['mask_partial_columns']:
        df[column] = df[column].mask(df[column].notnull(), df[column].str[:3] + '***' + df[column].str[-3:])
    return df

# drop columns
def drop_columns(df):
    return df.drop(columns=config['columns_to_drop'])

# rename columns 
def rename_columns(df):
    df.rename(columns=config['columns_to_rename'], inplace=True)
    return df

# compute columns
def compute_columns(df):
    # for column in config['compute_columns']:
    #     df[column] = df[column].apply(lambda x: x * 2)
    # return df
    pass

df = pd.read_csv('datasets/output_sample.csv')
df_transformed = (df
                  .pipe(load_data)
                  .pipe(modify_data_types)
                  .pipe(impute_mean)
                  .pipe(impute_mode)
                  .pipe(drop_na)
                  .pipe(split_columns)
                  .pipe(mask_full_columns)
                  .pipe(mask_partial_columns)
                  .pipe(rename_columns)
                  .pipe(drop_columns)
                  )



print(df_transformed.info())
df_transformed.to_csv('datasets/output_sample_transformed.csv', index=False)
