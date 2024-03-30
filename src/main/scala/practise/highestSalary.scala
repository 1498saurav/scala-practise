package practise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, row_number}

object highestSalary extends App {
val spark=SparkSession.builder().appName("").master("local[*]").getOrCreate()


  val df=spark.read.csv("")

  import spark.implicits._
  df.withColumn("rnk",dense_rank()
      .over(
        Window.partitionBy(col("dept_id"))
        .orderBy(col("salary").desc())
      )
    )
    .filter($"col"===2)
}
