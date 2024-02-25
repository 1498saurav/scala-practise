package practise

import org.apache.arrow.flatbuf.Interval
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, column, current_date, current_timestamp, explode, expr, lit, regexp_replace, split, to_date}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.Minutes

object MergeDataframes extends App {

  val spark=SparkSession
    .builder()
    .appName("ReadWeirdDelimiter")
    .master("local[*]")
    .getOrCreate()

  private val maps=Map(
    "header" -> "true",
    "inferSchema"-> "true"
  )

  /*val schema = StructType(Seq(
    StructField("Name",StringType,nullable = true),
    StructField("Age",IntegerType,nullable = true),
    StructField("City",StringType,nullable = true)
  ))*/

  val schema = StructType(Seq(
    StructField("Name",StringType,nullable = true),
    StructField("Age",IntegerType,nullable = true)
  ))

  private val df1=spark.read.options(maps).csv("src/main/resources/sample1.csv")
  private val df2=spark.read.options(maps).csv("src/main/resources/sample2.csv")
  //private val df2=spark.read.options(maps).schema(schema).csv("src/main/resources/sample2.csv")

  //df2.withColumn("city",lit(null)).union(df1).show(1,truncate = false)
  //df2.union(df1).show(1,truncate = false)

  //df1.join(df2,Seq("name","age"),"outer").show(false)

  //df1.withColumn("removedash",regexp_replace(current_date(),"-","")).withColumn("time",to_date(col("removedash"),"yyyyMMdd")).selectExpr("date_add(time,age)").show(false)
  //.withColumn("current_ts",current_timestamp()).selectExpr(s"age","date_sub(current_dt,Age)","current_ts - INTERVAL 1 Hour").show(false)

  val singleValueDF=spark.sql("select current_date as dt").withColumn("value",lit("A|1|B|2|C|3|D|4")).select("value")
  /*val explodedDF=singleValueDF
    .withColumn("addinglinebreak",regexp_replace(col("value"),"(.*?\\|){2}","$0-"))
    .withColumn("exploded",explode(split(col("addinglinebreak"),"-")))
    .select("exploded").rdd.map(row => {
      val Array(name, age) = row.getString(0).split("\\|")
      Row(name, age.toInt)
    }
    )*/

  val explodedDF=spark.sql("select current_date as dt").withColumn("value",lit("A|1|B|2|C|3|D|4|"))
    .withColumn("splitedColumn",explode(split(regexp_replace(col("value"),"(.*?\\|){2}","$0-"),"-"))).select("splitedcolumn")
    .rdd.flatMap(row=>{
      try{
          val Array(name,age) = row.getString(0).split("\\|")
          Some(Row(name,age.toInt))
        }catch {
        case _: Throwable => None
      }
      }
    )


 spark.createDataFrame(explodedDF,schema).show(false)

}
