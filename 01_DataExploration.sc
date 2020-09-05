import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types

//Start Spark Session
val spark = SparkSession.builder().appName("XGBoost on Spark").config("spark.hadoop.fs.s3a.s3guard.ddb.region","us-east-1").config("spark.yarn.access.hadoopFileSystems","s3a://demo-aws-1/").config("spark.hadoop.yarn.resourcemanager.principal",System.getenv("HADOOP_USER_NAME")).getOrCreate()

//USE THESE WHEN IDBROKER PROBLEM IS FIXED
//val trainDF = spark.sql("SELECT * FROM DEFAULT.SPRINGLEAF_TRAIN")
//val testDF = spark.sql("SELECT * FROM DEFAULT.SPRINGLEAF_TEST")
val custintDF = spark.read.option("header","true").csv("customer_interactions.csv")

//Checking the data 
custintDF.show(3)
custintDF.printSchema
custintDF.count

//All fields are of type String
custintDF.dtypes.groupBy(_._2).map{case (k,v) => (k, custintDF.select(v.map { x => col(x._1) }: _*))}

//Converting recency
val custintDF = custintDF.withColumn("recency", $"recency".cast(types.IntegerType))
val custintDF = custintDF.withColumn("history", $"history".cast(types.FloatType))
val custintDF = custintDF.withColumn("used_discount", $"used_discount".cast(types.IntegerType))
val custintDF = custintDF.withColumn("used_bogo", $"used_bogo".cast(types.IntegerType))
val custintDF = custintDF.withColumn("is_referral", $"is_referral".cast(types.IntegerType))
val custintDF = custintDF.withColumn("conversion", $"conversion".cast(types.IntegerType))
val custintDF = custintDF.withColumn("score", $"score".cast(types.FloatType))









//spark.stop()