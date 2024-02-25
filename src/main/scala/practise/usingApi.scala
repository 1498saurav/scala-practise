package practise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, split}
import scalaj.http.Http

object usingApi extends App{

  val spark=SparkSession.builder().appName("Test_Api").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("error")

  val url="https://randomuser.me/api/"

  private val response=Http(url).asString
  var data: String = _
  if(response.is2xx) {
    data=response.body
    println(data)
    //{"type":"general","setup":"Dad, can you put my shoes on?","punchline":"I don't think they'll fit me.","id":84}
  }

  import spark.implicits._


  val df=spark.read.json(Seq(data).toDS())
  df.printSchema

  df.withColumn("page",col("info.page"))
    .withColumn("result",explode(col("results")))
    .select("page","result.cell").show(false)


}
