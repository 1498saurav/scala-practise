package practise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

object consec extends App{

  val spark: SparkSession =SparkSession.builder().appName("consec").master("local[*]").getOrCreate()

  val options:Map[String,String]=Map(
    "header"->"true",
    "inferSchema"->"true",
    "delimiter"->"|"
  )

  spark.sparkContext.setLogLevel("error")

  private val df=spark.read.format("csv").options(options).load("src/main/resources/consec.csv").withColumn("num",col("num").cast("int"))

  val window=Window.partitionBy("num").orderBy(col("id"))

    df.withColumn("row_number",row_number().over(window))
      .withColumn("difference",col("id")-col("row_number"))
      .groupBy("num","difference").count()
      .filter("count>2")
      .select("num","count")
      .show(false)



}
