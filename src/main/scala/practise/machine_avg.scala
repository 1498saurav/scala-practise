package practise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object machine_avg extends App {

  val options:Map[String,String]=Map(
    "inferSchema" -> "true",
    "delimiter"->"|",
    "header"->"true"
    )

    val spark=SparkSession.builder().appName("machine_average").master("local[*]").getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("error")

    val df=spark.read.options(options).csv("src/main/resources/machine.csv")

    //Step 1 pivot column on basis of machine id and process id
  val df_pivot=df.groupBy(col("machine_id"),col("process_id")).pivot("process_time").sum().orderBy(col("machine_id"),col("process_id"))
    .select("machine_id","process_id","end_sum(timestamp)","start_sum(timestamp)")
    .withColumnRenamed("end_sum(timestamp)","end")
    .withColumnRenamed("start_sum(timestamp)","start")
  df_pivot.printSchema()

  df_pivot.show(false)

  val df_final=df_pivot
    .withColumn("difference",col("end")-col("start"))
    .groupBy("machine_id")
    .avg("difference")

    df_final
    .show(false)




}
