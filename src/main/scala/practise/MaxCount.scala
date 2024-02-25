package practise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, expr, row_number, sum}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object MaxCount extends App{

  val spark: SparkSession =SparkSession.builder().appName("Max Count").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("error")

  val options: Map[String, String] =Map (
    "inferSchema" -> "true",
    "header"->"true",
    "delimiter"->"\t"
  )

  val schema=StructType(Seq(
    StructField("id",IntegerType,false),
    StructField("cust_id",IntegerType,false),
    StructField("order_date",DateType,false),
    StructField("order_details",StringType,false),
    StructField("total_order_cost",IntegerType,false),
  ))

  import spark.implicits._

  val window=Window.orderBy($"total_order_cost".desc)

  spark.read.schema(schema).options(options).csv("src/main/resources/orders.csv")
    .filter($"order_date">= "2019-02-01" &&  $"order_date"<="2019-05-01")
    .groupBy("cust_id","order_date")
      .sum("total_order_cost").withColumnRenamed("sum(total_order_cost)","total_order_cost")
    .withColumn("rnk",row_number().over(window))
    .filter("rnk=1")
    .as("orders")
    .join(spark.read.options(options).csv("src/main/resources/customers.csv").as("customers")
    ,expr("orders.cust_id=customers.id")
    ).drop("orders.cust_id")
    .show(false)

}
