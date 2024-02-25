package practise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, to_date}

object PremiumVsFreemium extends App{
  val spark: SparkSession =SparkSession.builder().appName("Premium vs Freemium").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("error")

  val options: Map[String, String] =Map (
    "inferSchema" -> "true",
    "header"->"true",
    "delimiter"->"\t"
  )

  import  spark.implicits._
  spark.read.options(options).csv("src/main/resources/ms_user_dimension.csv")
    .join(spark.read.options(options).csv("src/main/resources/ms_acc_dimension.csv"),Seq("acc_id"))
    .join(spark.read.options(options).csv("src/main/resources/ms_download_facts.csv"),Seq("user_id"))
    .withColumn("date",to_date($"date")).orderBy("date","paying_customer")
    .groupBy("date").pivot("paying_customer").sum("downloads").orderBy("date").drop("paying_customer")
    .filter($"no">$"yes")
    .show(false)
}

