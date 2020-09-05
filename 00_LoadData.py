from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

#Start Spark Session
spark = SparkSession.builder.appName("XGBoost on Spark")\
        .config("spark.hadoop.fs.s3a.s3guard.ddb.region","us-east-1")\
        .config("spark.yarn.access.hadoopFileSystems","s3a://demo-aws-1/")\
        .config("spark.hadoop.yarn.resourcemanager.principal",os.getenv("HADOOP_USER_NAME"))\
        .getOrCreate()

trainDF = spark.read.option("header","true").csv("train.csv")
testDF = spark.read.option("header","true").csv("test.csv")

#Creating Spark Tables
trainDF.write.format("parquet").mode("overwrite").saveAsTable("default.Springleaf_train")
trainDF.write.format("parquet").mode("overwrite").saveAsTable("default.Springleaf_test")

spark.stop()