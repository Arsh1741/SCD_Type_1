import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce

spark = SparkSession.builder \
                    .appName("SCD_Type1") \
                    .getOrCreate()

existing_df = spark.read.format("csv") \
                        .option("header", "true") \
                        .load('files/existing_employees.csv')
source_df =  spark.read.format("csv") \
                        .option("header", "true") \
                        .load('files/existing_employees.csv')


joined_df = source_df.alias("source").join(
            existing_df.alias("existing"), on="EMPLOYEE_ID", how="outer")


primary_key = 'EMPLOYEE_ID'
update_df = joined_df.filter((col(f"existing.{primary_key}").isNotNull()) & \
                             (col(f"source.{primary_key}").isNotNull())) \
                     .select([coalesce(col(f"source.{col_name}"), col(f"existing.{col_name}")).alias(col_name) \
                              for col_name in source_df.columns])

insert_df = joined_df.filter((col(f"existing.{primary_key}").isNull()) & \
                             (col(f"source.{primary_key}").isNotNull())) \
                     .select([col(f"source.{col_name}").alias(col_name) \
                             for col_name in source_df.columns])

upsert_df = update_df.union(insert_df)

anti_df = existing_df.join(upsert_df, on='EMPLOYEE_ID', how='anti')

final_df = upsert_df.union(anti_df).sort('EMPLOYEE_ID')

final_df.write.option('header', True).mode('overwrite').csv('files')
