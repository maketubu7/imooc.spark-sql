package spark_sql

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.collection.mutable

object json_test3 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("json_test")

    val sc = SparkContext.getOrCreate(conf)

    val sk = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val path = "datas/score.json"

    val res: DataFrame = sk.sqlContext.read.json(path)

    res.printSchema()

    res.show()

  }

}
