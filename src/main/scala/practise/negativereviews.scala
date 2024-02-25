package practise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object negativereviews extends App{

  val spark=SparkSession.builder().appName("negative reviews").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("error")


  val options=Map (
    "inferSchema" -> "true",
    "header"->"true",
    "delimiter"->"\t"
  )

  private val stores=spark.read.format("csv").options(options).load("src/main/resources/stores.csv")
  //stores.printSchema()

  private val reviews=spark.read.format("csv").options(options).load("src/main/resources/reviews.csv")
  //reviews.printSchema()

  val df =reviews.withColumn("binaryResult",
    when(col("score")>5,1)
    .otherwise(0)
  ).groupBy("store_id").pivot("binaryResult").count()
    .withColumnRenamed("0","negatives")
    .withColumnRenamed("1","positives")
    .na.fill(0)
    .withColumn("percentage",(col("negatives")/(col("positives")+col("negatives")))*100)
    .withColumn("ratio",col("negatives")/col("positives"))
    .filter(col("percentage")>20).select("store_id")
    .as("rev").join(stores.as("st"),expr("st.id=rev.store_id")).filter(year(col("opening_date"))===2021 && month(col("opening_date"))>6).drop("store_id")

  /*  df.groupBy("store_id").sum("0","1").orderBy("store_id").na.fill(0)
      .withColumnRenamed("sum(0)","negatives")
      .withColumnRenamed("sum(1)","positives")
      .withColumn("percentage",(col("negatives")/(col("positives")+col("negatives")))*100)
      .filter(col("percentage")>20).select("store_id")
      .as("rev").join(stores.as("st"),expr("st.id=rev.store_id")).filter(year(col("opening_date"))===2021 && month(col("opening_date"))>6).drop("store_id")
      .show(false)*/


  /*val df =reviews.withColumn("binaryResult",
    when(col("score")>5,1)
      .otherwise(0)
  ).groupBy("store_id").agg(
    sum(when(col("binaryResult")===1,1)).as("positives"),
    sum(when(col("binaryResult")===0,1)).as("negatives"),
    count("binaryResult")
  ).show(false)*/

}
