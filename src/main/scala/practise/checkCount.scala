package practise

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object checkCount extends App{

  val spark: SparkSession =SparkSession.builder().appName("negative reviews").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("error")

  val options: Map[String, String] =Map (
    "inferSchema" -> "true",
    "header"->"true",
    "delimiter"->"\t"
  )

  private val employee = Seq(
    (1, "Sagar" ,23),
    (2, null , 34),
    (null ,"John" , 46),
    (5,"Alex", null) ,
    (4,"Alice",null)
  )

  private val data= employee.map(
    row => {
      val x=Row(row._1,row._2,row._3)
      x
    }
  )

  val nullset=spark.sparkContext.parallelize(data)

  val schema = StructType(Seq(
    StructField("emp_id",IntegerType,nullable = true),
    StructField("name",StringType,nullable = true),
    StructField("age",IntegerType,nullable = true)
  ))

  val map = scala.collection.mutable.Map[String,Int]()
  map("emp_id")=0
  map("age")=0
  map("name")=0

  val x=spark.createDataFrame(nullset,schema).collect()
    .map(row=>{
      val emp_id=row.get(0)
      val name=row.get(1)
      val age=row.get(2)

      try{
       emp_id.toString.isEmpty
      }catch {
        case _:Exception =>
          map("emp_id")=map("emp_id")+1
      }

      try{
        name.toString.isEmpty
      }catch {
        case _:Exception =>
          map("name")=map("name")+1
      }

      try{
        age.toString.isEmpty
      }catch {
        case _:Exception =>
          map("age")=map("age")+1
      }

//      Some(emp_id,name,age)
    })

    println(map)

  private val employee2 = Seq(
    Row(1, "Sagar" ,23),
    Row(2, null , 34),
    Row(null ,"John", 46),
    Row(5,"Alex", null),
    Row(4,"Alice",null)
  )


  private val result=spark.createDataFrame(spark.sparkContext.parallelize(employee2),schema).collect().map(row=>{
    Option(row.getAs[Int]("emp_id")).getOrElse("null")
  })
    //.show(false)

  println(result)

}


