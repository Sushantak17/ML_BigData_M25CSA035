from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Q10_Metadata").getOrCreate()

books_df = spark.read.text("file:///Users/sushantakparasharjha/D184MB/")
books_df = books_df.withColumnRenamed("value","text") \
                   .withColumn("file_name", input_file_name())

metadata_df = books_df \
    .filter(
        lower(col("text")).startswith("title:") |
        lower(col("text")).startswith("release date:") |
        lower(col("text")).startswith("language:") |
        lower(col("text")).startswith("encoding:")
    ) \
    .groupBy("file_name") \
    .agg(
        max(when(lower(col("text")).startswith("title:"), col("text"))).alias("title"),
        max(when(lower(col("text")).startswith("release date:"), col("text"))).alias("release_date"),
        max(when(lower(col("text")).startswith("language:"), col("text"))).alias("language"),
        max(when(lower(col("text")).startswith("encoding:"), col("text"))).alias("encoding")
    )

metadata_df.show(5, False)

books_per_year = metadata_df \
    .withColumn("year", regexp_extract(col("release_date"), "(\\d{4})", 1)) \
    .groupBy("year").count().orderBy("year")

books_per_year.show()

metadata_df.groupBy("language").count().orderBy(desc("count")).show(5)

metadata_df \
    .withColumn("title_length", length(col("title"))) \
    .select(avg("title_length")).show()
