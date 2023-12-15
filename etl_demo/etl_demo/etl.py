from transformations import Transformation, OklahomaDatasetTransformation, TexasDatasetTransformation
from pyspark.sql import SparkSession
from pyspark.sql import dataframe
import typing


OUTPUT_COLUMNS = [
    "accepts_financial_aid",
    "ages_served",
    "capacity",
    "certificate_expiration_date",
    "city",
    "address1",
    "address2",
    "company",
    "phone1",
    "phone2",
    "county",
    "curriculum_type",
    "email",
    "first_name",
    "last_name",
    "language",
    "license_type",
    "license_status",
    "license_issued"
]


class DataFrameBuilder:
    def __init__(self, spark_session: SparkSession, input_paths: typing.List, transformations: typing.List[Transformation]):
        self.spark = spark_session
        self.transformations = transformations
        self.dataframes = self.parse_datasets(paths=input_paths)

    def build(self, source_dataframe: dataframe.DataFrame, transformation_index: int) -> dataframe.DataFrame:
        return self.transformations[transformation_index].transform(source_dataframe=source_dataframe)

    def parse_datasets(self, paths: typing.List) -> typing.List[dataframe.DataFrame]:
        return [self.spark.read.parquet(path) for path in paths]


def run_etl(df_builder: DataFrameBuilder, columns: typing.List) -> None:
    results = []
    try:
        for item, dataframe in enumerate(df_builder.dataframes):
            filtered_source_dataframe = dataframe.select(columns[item])
            result_dataframe = (
                df_builder
                .build(source_dataframe=filtered_source_dataframe, transformation_index=item)
            )
            results.append(result_dataframe.select(OUTPUT_COLUMNS))

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
        "Address",
        "Operation/Caregiver Name",
        "Capacity",
        "City",
        "Infant",
        "Toddler",
        "Preschool",
        "School",
        "County",
        "Email Address",
        "Type",
        "Status",
        "Phone",
        "Issue Date"
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
