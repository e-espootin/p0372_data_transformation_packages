from pyspark.sql import SparkSession, DataFrame, spark
from pyspark.sql.functions import col, upper


# Define transformations as functions
def add_one_to_id(df):
    return df.withColumn("id_plus_one", col("id") + 1)

def uppercase_name(df):
  return df.withColumn("name_upper", upper(col("name")))


def ingest_df(spark: SparkSession):
    # Sample DataFrame
    df = spark.createDataFrame([
        (1, "John"),
        (2, "Jane")
    ], ["id", "name"])

    result = (
        df
        .transform(add_one_to_id)
        .transform(uppercase_name)
    )

    display(result)
    return result




def main():
  ingest_df()

if __name__ == '__main__':
  main()
