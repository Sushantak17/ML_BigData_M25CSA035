from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Q12_Influence").getOrCreate()

books_df = spark.read.text("file:///Users/sushantakparasharjha/D184MB/")
books_df = books_df.withColumnRenamed("value","text") \
                   .withColumn("file_name", input_file_name())

author_df = books_df \
    .filter(
        lower(col("text")).startswith("author:") |
        lower(col("text")).startswith("release date:")
    ) \
    .groupBy("file_name") \
    .agg(
        max(when(lower(col("text")).startswith("author:"), col("text"))).alias("author"),
        max(when(lower(col("text")).startswith("release date:"), col("text"))).alias("release_date")
    )

author_year_df = author_df \
    .filter(col("author").isNotNull()) \
    .withColumn("year_str", regexp_extract(col("release_date"), "(\\d{4})", 1)) \
    .filter(col("year_str") != "") \
    .withColumn("year", col("year_str").cast("int"))

X = 5

edges = author_year_df.alias("a").join(
    author_year_df.alias("b"),
    (col("b.year") > col("a.year")) &
    ((col("b.year") - col("a.year")) <= X) &
    (col("a.author") != col("b.author"))
).select(
    col("a.author").alias("author1"),
    col("b.author").alias("author2")
)

out_degree = edges.groupBy("author1").count().withColumnRenamed("count","out_degree")
in_degree = edges.groupBy("author2").count().withColumnRenamed("count","in_degree")

out_degree.orderBy(desc("out_degree")).show(5, False)
in_degree.orderBy(desc("in_degree")).show(5, False)
