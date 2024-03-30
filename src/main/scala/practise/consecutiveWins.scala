package practise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, dense_rank, desc, row_number}

object consecutiveWins extends App{
  val spark: SparkSession =SparkSession.builder().appName("negative reviews").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("error")


  val options: Map[String, String] =Map (
    "inferSchema" -> "true",
    "header"->"true",
    "delimiter"->"\t"
  )

  private val tennis=spark.read.format("csv").options(options).load("src/main/resources/tennis.csv")
  tennis.printSchema()

/* /* val df= */
 val arrays=tennis.orderBy("player_id", "match_date").collect().map(row => {
    val playerID = row.getInt(0)
    val match_date = row.getTimestamp(1)
    val matchResult = row.getString(2)
    (playerID, match_date, matchResult)
  })

  private var winCount=0
  private var currentUser=0
  var map = scala.collection.mutable.Map[String, String]()

  arrays.foreach(row=>{

    if(currentUser==0 || currentUser!=row._1) {
      if(winCount!=0 && winCount>map(currentUser.toString).toInt){
        map(currentUser.toString)=winCount.toString
      }

      currentUser=row._1
      winCount=0
      map(currentUser.toString)=0.toString
    }

    if(row._3.equals("W")){
      winCount+=1
    }else if(currentUser==row._1 && row._3.equals("L")){
      if(winCount>map(currentUser.toString).toInt)
        map(currentUser.toString)=winCount.toString
      winCount=0
    }
    currentUser=row._1

  })

  spark.createDataFrame(map.toList)
    .withColumnRenamed("_1","player_id")
    .withColumnRenamed("_2","Max Streak")
   .show()


*/



  var df = tennis.withColumn("rn" , row_number().over(Window.partitionBy(col("player_id")).orderBy("match_date")))
    .filter(col("match_result")==="W")

  df = df
    .withColumn("rn_diff" , col("rn") - row_number().over(Window.partitionBy(col("player_id")).orderBy("match_date")))
  df.show(100,false)

  df = df
    .groupBy(col("player_id") , col("rn_diff"))
    .agg(count(col("match_date")).alias("winning_streak_count"))
  df.show(100,false)



  df = df.withColumn("winners_rank" , dense_rank().over(Window.orderBy(desc("winning_streak_count"))))
    .filter(col("winners_rank")===1)
    .select(col("player_id"), col("winning_streak_count"))
  df.show(100,false)


  df.show(false)
}
