package practise

import org.apache.spark.sql.SparkSession

object consec extends App{

  val spark: SparkSession =SparkSession.builder().appName("consec").master("local[*]").getOrCreate()

  val options:Map[String,String]=Map(
    "headers"->"true",
    "inferSchema"->"true"
  )

  private val df=spark.read.format("csv").options(options).load("src/main/resources/consec.csv")
  df.show(false)

}
