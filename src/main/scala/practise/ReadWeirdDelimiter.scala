package practise

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object ReadWeirdDelimiter extends App {

  val spark=SparkSession
    .builder()
    .appName("ReadWeirdDelimiter")
    .master("local[*]")
    .getOrCreate()

  /*private val df=spark.sparkContext.textFile("src/main/resources/sample.csv")
  private val skipHeader=df.first()
  private val rowConvert=df.filter(row => row!=skipHeader).map(line => Row.fromSeq(line.split("~\\|")))

  val schema = StructType(Seq(
    StructField("Name", StringType, nullable = true),
    StructField("Age", StringType, nullable = true),
    StructField("City", StringType, nullable = true),
  ))

  spark.createDataFrame(rowConvert,schema).show(false)
*/
  import spark.implicits._

  private val df=spark.read.textFile("src/main/resources/sample.csv")
  val a=df.map( line => line.mkString.split("~\\|").mkString("\t"))
  a.show()
}
