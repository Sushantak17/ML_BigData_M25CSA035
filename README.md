# Machine Learning with Big Data — Assignment 1

Student Name: Sushantak Parashar Jha  
Roll Number: M25CSA035  
Course: Machine Learning with Big Data  
Assignment: Hadoop MapReduce and Spark Processing

--------------------------------------------------

## Overview
This repository contains the implementation and experimental results for Assignment-1, which focuses on large-scale text processing using Apache Hadoop MapReduce and Apache Spark.

The assignment demonstrates:
- Hadoop WordCount MapReduce execution
- HDFS data handling
- Execution time analysis
- Metadata extraction from large text collections
- TF-IDF computation using Spark
- Book similarity analysis using cosine similarity
- Author influence network construction

--------------------------------------------------

## Repository Structure
WordCount.java  
M25CSA035_CSL7110_Assignment.pdf  

spark_pyspark/
    q10_metadata_extraction.py  
    q11_tfidf_similarity.py  
    q12_author_influence.py  

--------------------------------------------------

## Technologies Used
- Apache Hadoop (HDFS, MapReduce)
- Apache Spark (PySpark)
- Python
- Java

--------------------------------------------------

## Execution

### Hadoop WordCount
Compile:
javac -classpath `hadoop classpath` WordCount.java  
jar cf wordcount.jar WordCount*.class  

Run:
hadoop jar wordcount.jar WordCount input output  

### Spark Programs
spark-submit spark_pyspark/q10_metadata_extraction.py  
spark-submit spark_pyspark/q11_tfidf_similarity.py  
spark-submit spark_pyspark/q12_author_influence.py  

--------------------------------------------------

## Report
All screenshots, observations, explanations, and results are provided in:

M25CSA035_CSL7110_Assignment.pdf

--------------------------------------------------

## Notes
The dataset used for processing (Project Gutenberg books) is not included in the repository due to large size. It should be configured locally before execution.
