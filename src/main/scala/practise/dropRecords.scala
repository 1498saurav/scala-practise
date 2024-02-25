package practise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import practise.consecutiveWins.{options, spark}

object dropRecords extends App {

  val spark: SparkSession =SparkSession.builder().appName("negative reviews").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("error")

  val options: Map[String, String] =Map (
    "header"->"false",
    "delimiter"->"|",
  )

  private val phone=spark.read.format("csv").options(options).load("src/main/resources/phone.csv").withColumnRenamed("_c2","phoneno")
    .withColumn("phoneNumber",col("phoneno").cast(LongType)).filter(col("phoneNumber").isNotNull).drop("phoneNumber").show(false)

  //phone.show(false)



}


