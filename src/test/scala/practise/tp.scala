package practise

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Test
import org.scalatest.FunSuite

import java.awt.datatransfer.DataFlavor

class tp extends FunSuite {

  val spark: SparkSession =SparkSession.builder().appName("tp").master("local[*]").getOrCreate()

  def defaultPartitionsTestCase(): RDD[Int] = {
    spark.sparkContext.emptyRDD
  }

  test(":P"){
    assert(
      defaultPartitionsTestCase().count()==spark.sparkContext.parallelize((1 to 10).toList).count()
    )
  }


}
