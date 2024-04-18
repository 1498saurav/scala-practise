package practise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_list, regexp_replace, struct, to_json}

object json extends App{

  val spark= SparkSession.builder().appName("json").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val map:Map[String,String]=Map(
    "inferSchema"->"true",
    "header"->"true",
    "delimiter"->"\t"
  )

  val df=spark.read.options(map).csv("src/main/resources/customers.csv").limit(2)
    //.write.mode("overwrite").json("src/main/resources/customers1.csv")


   df.show(false)
   df.printSchema()


  /*val test = df.select("id","first_name", "last_name","phone_number").agg(collect_list(to_json(struct("first_name", "last_name"))).alias("names"))
     .selectExpr("collect_list(to_json(struct(id,phone_number,names))) as data")
*/



  val test = df.groupBy("id","phone_number").agg(
    collect_list(
      struct(
        df("first_name").alias("first_name"),
        df("last_name").alias("last_name")
      )
    ).alias("names")
  ).toJSON

  //val test=df.selectExpr("to_json(collect_list(to_json(struct(id,struct(first_name,last_name),phone_number)))) as data")
  test.show(false)
     //test.coalesce(1).write.mode("overwrite").text("src/main/resources/customers1.json")
     //.show(false)

/***working***/
  /*df.selectExpr("to_json(collect_list(struct(id,phone_number,struct(first_name,last_name)))) as data")
    .write.mode("overwrite").text("src/main/resources/customers1.json")*/

  df.selectExpr("to_json(collect_list(struct(id,phone_number,struct(first_name,last_name) as names))) as data")
    .write.mode("overwrite").text("src/main/resources/customers1.json")

    //.show(false)

  val t=spark.read.options(map).json("src/main/resources/customers1.json")
    t.printSchema()
    t.select("id","names.first_name").show(false)

}
