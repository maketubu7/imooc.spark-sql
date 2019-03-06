package spark_sql

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._

import scala.collection.mutable

object json_test2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("json_test")

    val sc = SparkContext.getOrCreate(conf)

    val sk = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val schema = StructType(Seq(
      StructField("id", LongType),
      StructField("package", StringType),
      StructField("activetime", LongType)
    ))

    val encoder = RowEncoder(schema)

    val df = sk.read.json("datas/data.json")
      .flatMap(new FlatMapFunction[Row, Row] {
        override def call(r: Row): util.Iterator[Row] = {
          val list = new util.ArrayList[Row]()
          val datas = r.getAs[mutable.WrappedArray.ofRef[Row]]("data")
          datas.foreach(data => {
            list.add(Row(r.getAs[Long]("id"), data.getAs[Long](1), data.getAs[String](0)))
          })
          list.iterator()
        }
      }, encoder)

    df.show()

  }
}
