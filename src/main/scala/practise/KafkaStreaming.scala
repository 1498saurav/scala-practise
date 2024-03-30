package practise

import org.apache.spark.sql.SparkSession

object KafkaStreaming extends App{

  val spark=SparkSession.builder().appName("Kafka Streaming").master("local[*]").getOrCreate()

  val option=Map(
    ""->"",
  )



}
