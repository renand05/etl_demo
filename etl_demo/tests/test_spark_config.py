from pyspark.sql import SparkSession

def test_spark_configuration():
    spark = SparkSession.builder.appName("TestSparkConfiguration").getOrCreate()

    df = spark.createDataFrame([(1, 'Alice'), (2, 'Bob')], ['id', 'name'])
    result = df.select('name').collect()

    assert result == [('Alice',), ('Bob',)]

    spark.stop()
