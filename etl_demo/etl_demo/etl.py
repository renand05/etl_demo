from transformations import Transformation, OklahomaDatasetTransformation, TexasDatasetTransformation
from pyspark.sql import SparkSession
from pyspark.sql import types, dataframe
import typing


class DataFrameBuilder:
    def __init__(self, spark_session: SparkSession, input_paths: typing.List, transformations: typing.List[Transformation]):
        self.spark = spark_session
        self.transformations = transformations
        self.dataframes = self.parse_datasets(paths=input_paths)
        self.schema = self.set_output_schema()

    def set_output_schema(self) -> types.StructType:
        return types.StructType([
            types.StructField("accepts_financial_aid", types.StringType(), True),
            types.StructField("ages_served", types.StringType(), True),
            types.StructField("capacity", types.DoubleType(), True),
            types.StructField("certificate_expiration_date", types.DateType(), True),
            types.StructField("city", types.StringType(), True),
            types.StructField("address1", types.StringType(), True),
            types.StructField("address2", types.StringType(), True),
            types.StructField("company", types.StringType(), True),
            types.StructField("phone1", types.StringType(), True),
            types.StructField("phone2", types.StringType(), True),
            types.StructField("county", types.StringType(), True),
            types.StructField("curriculum_type", types.StringType(), True),
            types.StructField("email", types.StringType(), True),
            types.StructField("first_name", types.StringType(), True),
            types.StructField("last_name", types.StringType(), True),
            types.StructField("language", types.StringType(), True),
            #types.StructField("license_status", types.StringType(), True),
            #types.StructField("license_issued", types.DateType(), True),
            #types.StructField("license_number", types.DoubleType(), True),
            #types.StructField("license_renewed", types.DateType(), True),
            #types.StructField("license_type", types.StringType(), True),
            #types.StructField("licensee_name", types.StringType(), True),
            #types.StructField("max_age", types.DoubleType(), True),
            #types.StructField("min_age", types.DoubleType(), True),
            #types.StructField("operator", types.StringType(), True),
            #types.StructField("provider_id", types.StringType(), True),
            #types.StructField("schedule", types.StringType(), True),
            #types.StructField("state", types.StringType(), True),
            #types.StructField("title", types.StringType(), True),
            #types.StructField("website_address", types.StringType(), True),
            #types.StructField("zip", types.StringType(), True),
            #types.StructField("facility_type", types.StringType(), True),
        ])

    def build(self, source_dataframe: dataframe.DataFrame, transformation_index: int) -> dataframe.DataFrame:
        return self.transformations[transformation_index].transform(source_dataframe=source_dataframe)

    def parse_datasets(self, paths: typing.List) -> dataframe.DataFrame:
        return [self.spark.read.parquet(path) for path in paths]


def run_etl(df_builder: DataFrameBuilder, columns: typing.List) -> None:
    try:
        for item, dataframe in enumerate(df_builder.dataframes):
            filtered_source_dataframe = dataframe.select(columns[item])
            result_dataframe = (
                df_builder
                .build(source_dataframe=filtered_source_dataframe, transformation_index=item)
            )
            result_dataframe.show()
    finally:
        spark.stop()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("ETLDemo").getOrCreate()
    oklahoma = [
        "Accepts Subsidy",
        "Ages Accepted 1",
        "AA2",
        "AA3",
        "AA4",
        "Total Cap",
        "License Monitoring Since",
        "City",
        "Address1",
        "Address2",
        "Company",
        "Phone",
        "Email",
        "Primary Caregiver",
        "State",
        "Type License",
        "Zip"
    ]
    texas = [
        "Operation/Caregiver Name",
        "Capacity",
        "Infant",
        "Toddler",
        "Preschool",
        "School"
    ]
    relevant_columns = [oklahoma, texas]
    input_paths = [
        "etl_demo/demo_data/Oklahoma.parquet",
        "etl_demo/demo_data/Texas.parquet"
    ]
    run_etl(
        df_builder=DataFrameBuilder(spark_session=spark, input_paths=input_paths, transformations=[OklahomaDatasetTransformation(), TexasDatasetTransformation()]),
        columns=relevant_columns,
    )