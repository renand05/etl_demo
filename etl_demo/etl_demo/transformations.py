from abc import ABC, abstractmethod
from pyspark.sql.functions import col, coalesce, concat, lit, split, upper, when
from pyspark.sql import dataframe 


class Transformation(ABC):
    @abstractmethod
    def transform(self):
        pass


class OklahomaDatasetTransformation(Transformation):
    def transform(self, source_dataframe: dataframe.DataFrame) -> dataframe.DataFrame:
        output_dataframe = (
            source_dataframe
            .withColumn('accepts_financial_aid', upper(coalesce(col('Accepts Subsidy'), lit('DOES NOT ACCEPT SUBSIDY'))))
            .withColumn("ages_served",
                concat(
                    upper(coalesce(concat(split(col("Ages Accepted 1"), " ").getItem(0), lit("|")), lit(""))),
                    upper(coalesce(concat(split(col("AA2"), " ").getItem(0), lit("|")), lit(""))),
                    upper(coalesce(concat(split(col("AA2"), " ").getItem(0), lit("|")), lit(""))),
                    upper(coalesce(concat(split(col("AA3"), " ").getItem(0), lit("|")), lit(""))),
                    upper(coalesce(concat(split(col("AA4"), " ").getItem(0), lit("|")), lit("")))
                )
            )
            .withColumn("capacity", col("Total Cap"))
            .withColumn("certificate_expiration_date", lit("NULL"))
            .withColumn("city", upper(coalesce(col("City"), lit("NULL"))))
            .withColumn("address1", upper(coalesce(col("Address1"))))
            .withColumn("address2", upper(coalesce(col("Address2"))))
            .withColumn("company", upper(coalesce(col("Company"))))
            .withColumn("phone1", upper(coalesce(col("Phone"))))
            .withColumn("phone2", lit("NULL"))
            .withColumn("county", lit("NULL"))
            .withColumn("curriculum_type", lit("NULL"))
            .withColumn("email", upper(coalesce(col("Email"))))
            .withColumn("first_name", upper(split(col("Primary Caregiver"), " ").getItem(0)))
            .withColumn("last_name", upper(split(col("Primary Caregiver"), " ").getItem(1)))
            .withColumn("language", lit("NULL"))

        )

        return output_dataframe


class TexasDatasetTransformation(Transformation):
    def transform(self, source_dataframe: dataframe.DataFrame) -> dataframe.DataFrame:
        output_dataframe = (
            source_dataframe
            .withColumn("accepts_financial_aid", lit("NULL"))
            .withColumn("ages_served", 
                coalesce(concat(
                    when(col("Infant") == "Y", "INFANT|").otherwise(""),
                    when(col("Toddler") == "Y", "TODDLER|").otherwise(""),
                    when(col("Preschool") == "Y", "PRESCHOOL|").otherwise(""),
                    when(col("School") == "Y", "SCHOOL|").otherwise(""),
                ), lit("NULL"))
            )
            .withColumn("capacity", col("Capacity"))
        )

        return output_dataframe