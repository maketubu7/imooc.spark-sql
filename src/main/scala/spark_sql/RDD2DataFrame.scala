package spark_sql


import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}




case class person(name:String,age:Int)
object RDD2DataFrame extends App {
  val sc = SparkUtil.createSparkContext(true,"rdd2dataframe")

  val sk = SparkSession.builder()
    .master("local[*]")
    .appName("rdd2dataframe")
    .getOrCreate()

  val json_path = "datas/dataframe.json"
  val json_path1 = "datas/class.json"
  val json_dataframe: DataFrame = sk.sqlContext.read.json(json_path)
  val json_class: DataFrame = sk.sqlContext.read.json(json_path1)


json_dataframe.show()

  //{"id":11,"data":[{"package":"com.browser1","activetime":60000},{"package":"com.browser6","activetime":1205000},{"package":"com.browser7","activetime":1205000}]}
  //json_class.printSchema()

  /*val jsonrdd: RDD[Row] = json_dataframe.rdd

  val res: RDD[(String, String)] = jsonrdd.map(row =>{
   // (row.getAs[String](0),row.getAs[Int](1))
    ("name:"+row.getAs[String]("name"),"age:"+row.getAs[Int]("students"))
  })

  println("======================")
  res.foreach(println)
  import sk.implicits._
//  json_dataframe.show()
//  res.foreach(println)
/*RDD转换dataframe
* 要导入sparksession的隐式转换
* import sk.implicits._
* */

 /* val rdd: RDD[person] = sc.parallelize(Array(
    person("路飞",18),
    person("鹰眼",32),
    person("四皇",80),
    person("索隆",25)

  ))

  val json_df = rdd.toDF()

  json_df.createOrReplaceTempView("people")

  val res_df: DataFrame = sk.sql("select * from people")

  res_df.show()*/

  /**
    * \第二种转换方式，RDD2Dataframe
    * 明确给定字段和schema信息
    */
  val rdd2: RDD[Row] = sc.parallelize(Array(
    ("路飞",18,"man"),
    ("鹰眼",32,"man"),
    ("四皇",80,"man"),
    ("索隆",25,"man")
  )).map({
    case (name,age,gender) =>{
      Row(name,age,gender)
    }
  })

  val  schema2 = StructType(Array(
    StructField("rddname",StringType,true),
    StructField("rddage",IntegerType,true),
    StructField("rddgender",StringType,true)
  ))

  val df2: DataFrame = sk.sqlContext.createDataFrame(rdd2,schema2)

  df2.show()
*/

}
