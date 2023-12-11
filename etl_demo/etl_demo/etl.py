from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def run_etl():
    spark = SparkSession.builder \
        .appName("ETL Demo") \
        .getOrCreate()

    try:
        csv_file_path = "etl_demo/demo_data/Nevada_Dept_of_Public_Behavioral_Health.csv"

        df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
        print("Transformed DataFrame:")
        df.show()

    finally:
        spark.stop()

if __name__ == "__main__":
    run_etl()