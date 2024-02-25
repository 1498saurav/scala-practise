package practise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, round, to_date}
import org.apache.spark.sql.types.StringType

object MonthlyTransactions extends App {

  val spark: SparkSession =SparkSession.builder().appName("negative reviews").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("error")


  val options: Map[String, String] =Map (
    "inferSchema" -> "true",
    "header"->"true",
    "delimiter"->"\t"
  )

  private val transactions=spark.read.format("csv").options(options).load("src/main/resources/transactions.csv")
 /* transactions.orderBy("id").dropDuplicates().show(false)
  transactions.printSchema()
*/
  val df=transactions.withColumn("year_month",to_date(col("created_at")).cast(StringType).substr(0,7))

  df.groupBy("year_month").sum("value").orderBy("year_month").withColumnRenamed("sum(value)","current_revenue")
    .withColumn("past_revenue",lag(col("current_revenue"),1).over(Window.orderBy("year_month")))
    .withColumn("revenue_diff_pct",round(((col("current_revenue")-col("past_revenue"))/col("past_revenue"))*100,2)).na.fill("")
    .select("year_month","revenue_diff_pct")
    .show(false)
}