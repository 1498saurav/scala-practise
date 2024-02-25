package practise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate.Min

import java.lang.Thread.sleep

object test {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
      .appName("chapter1")
      .master("local[*]")
      .getOrCreate()

    val sc=spark.sparkContext

    val x=(1 to 10).toList

    val rdd=sc.parallelize(x)
    println(rdd.count(),rdd.getNumPartitions)


    //println(rdd.reduce((a,b)=>(a*b)))

    //sleep(10000)
  }
}