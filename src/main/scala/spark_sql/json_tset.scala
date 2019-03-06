package spark_sql

import java.util

import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable


object json_tset {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("json_test")

    val sc = SparkContext.getOrCreate(conf)

    val sk = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    import sk.implicits._

    val json_path = "datas/class.json"
    val json_int_path = "datas/class_int.json"
    val json_class: DataFrame = sk.sqlContext.read.json(json_path)
    val json_class_int: DataFrame = sk.sqlContext.read.json(json_int_path)

//    json_class.printSchema()
//    json_class.show()

    print("========================")

    //    "age": 25,
    //    "gender": "female",
    //    "grades": "三班",
    //    "name": "露西",
    //    "score": 99,
    //    "weight": 51.3




  /*  val schema = StructType(Seq(
      StructField("cname", StringType),
      StructField("floor", StringType),
      StructField("age", StringType),
      StructField("gender", StringType),
      StructField("grades", StringType),
      StructField("name", StringType),
      StructField("score", StringType),
      StructField("weight", StringType)
    ))

    val encoder = RowEncoder(schema)

    val res: Dataset[Row] = json_class.flatMap(new FlatMapFunction[Row, Row] {
      override def call(r: Row): util.Iterator[Row] = {
        val list = new util.ArrayList[Row]()
        val datas = r.getAs[mutable.WrappedArray.ofRef[Row]]("student")
        datas.foreach(data => {
          list.add(Row(
            r.getAs[String]("cname"),
            r.getAs[String]("floor"),
            data.getAs[String]("grades"),
            data.getAs[String]("name"),
            data.getAs[String]("gender"),
            data.getAs[String]("age"),
            data.getAs[String]("weight"),
            data.getAs[String]("score")))
        })
        list.iterator()
      }
    }, encoder)

    res.show()
  }*/

    json_class_int.printSchema()

    json_class_int.show()

    val pre = json_class_int.select(json_class_int("cname"),
      json_class_int("floor"),
      explode(json_class_int("student"))).toDF("cname","floor","student")
    val res: DataFrame = pre.select("cname","floor","student.age","student.gender",
      "student.grades","student.name","student.score","student.weight")

/*    val pre_class: DataFrame = json_class.select(json_class("cname"),
      json_class("floor"),
      explode(json_class_int("student"))).toDF("cname","floor","student")*/


    res.show()

 //   val mid_class: DataFrame = pre_class.select("student.name")

//    val res_class = pre_class.select("cname","floor","student.age","student.gender",
//      "student.grades","student.name","""explode("student.score")""","student.weight")

//    res.printSchema()
//    res.show()

//    mid_class.printSchema()
//    mid_class.show()

 }
}
