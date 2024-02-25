package diksha

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, month, round, substring, to_date, year}

object monthTransactions extends App {

  val spark: SparkSession =SparkSession.builder().appName("negative reviews").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("error")

  val options: Map[String, String] =Map (
    "inferSchema" -> "true",
    "header"->"true",
    "delimiter"->"\t"
  )

  val window=Window.orderBy("year_month")
  //.partitionBy("year_month")

  import spark.implicits._
  spark.read.options(options).csv("src/main/resources/transactions.csv")
    .withColumn("year_month",substring(to_date($"created_at").cast("string"),0,7))
    .groupBy("year_month").sum("value").withColumnRenamed("sum(value)","value")
    .withColumn("last_value",lag("value",1).over(window))
    .withColumn("revenue_diff_pct",round((($"value"-$"last_value")/$"last_value")*100,2))
    .drop("value","revenue_diff_pct")
    .show(false)

}
