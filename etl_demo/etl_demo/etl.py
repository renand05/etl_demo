from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def run_etl():
    # Initialize a Spark session
    spark = SparkSession.builder \
        .appName("ETL Demo") \
        .getOrCreate()

    try:
        # Load data (replace with your data source)
        input_data = [("Alice", 25), ("Bob", 30), ("Charlie", 22)]
        df = spark.createDataFrame(input_data, ["Name", "Age"])

        # Transform data (example transformation: add 5 to the age)
        transformed_df = df.withColumn("AgePlus5", col("Age") + 5)

        # Show the transformed DataFrame
        print("Transformed DataFrame:")
        transformed_df.show()

        # Write the transformed data to an output location (replace with your destination)
        transformed_df.write.mode("overwrite").parquet("output_data")

    finally:
        # Stop the Spark session to release resources
        spark.stop()

if __name__ == "__main__":
    run_etl()