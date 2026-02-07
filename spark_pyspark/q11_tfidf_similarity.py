from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF, Normalizer
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.appName("Q11_TFIDF").getOrCreate()

books_df = spark.read.text("file:///Users/sushantakparasharjha/D184MB/")
books_df = books_df.withColumnRenamed("value","text") \
                   .withColumn("file_name", input_file_name())

clean_df = books_df.withColumn(
    "clean_text",
    lower(regexp_replace(col("text"), "[^a-zA-Z\\s]", " "))
)

tokenizer = RegexTokenizer(inputCol="clean_text", outputCol="words", pattern="\\W")
words_df = tokenizer.transform(clean_df)

remover = StopWordsRemover(inputCol="words", outputCol="filtered")
filtered_df = remover.transform(words_df)

cv = CountVectorizer(inputCol="filtered", outputCol="tf")
cv_model = cv.fit(filtered_df)
tf_df = cv_model.transform(filtered_df)

idf = IDF(inputCol="tf", outputCol="tfidf")
idf_model = idf.fit(tf_df)
tfidf_df = idf_model.transform(tf_df)

normalizer = Normalizer(inputCol="tfidf", outputCol="norm_features")
norm_df = normalizer.transform(tfidf_df)

book_vectors = norm_df.groupBy("file_name") \
    .agg(first("norm_features").alias("norm_features"))

target_vector = book_vectors \
    .filter(col("file_name").contains("10.txt")) \
    .select("norm_features") \
    .head()[0]

cosine_udf = udf(lambda v: float(v.dot(target_vector)), DoubleType())

similarity_df = book_vectors.withColumn(
    "similarity",
    cosine_udf(col("norm_features"))
)

similarity_df \
    .filter(~col("file_name").contains("10.txt")) \
    .orderBy(desc("similarity")) \
    .show(5, False)
