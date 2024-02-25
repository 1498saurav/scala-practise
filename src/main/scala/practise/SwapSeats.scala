package practise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SwapSeats extends App{

  val spark: SparkSession =SparkSession.builder().appName("negative reviews").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("error")


  val options: Map[String, String] =Map (
    "inferSchema" -> "true",
    "header"->"true",
    "delimiter"->"\t"
  )
  
  val data = Seq((1,"Abbot"),(2,"Doris"),(3,"Emerson"),(4,"Green"),(5,"Jeames"))

  val df = spark.createDataFrame(data.toList).withColumnRenamed("_1","id").withColumnRenamed("_2","Name")

  df.withColumn("Lead",lead(col("name"),1).over(Window.orderBy("id"))).withColumn("Lag",lag(col("name"),1).over(Window.orderBy("id")))
    .withColumn("exchangedSeats",
      when(col("id")%2===1,coalesce(col("lead"),col("name")))
      .otherwise(col("lag"))
    ).drop("Lead","Lag")
    .show(false)

}
