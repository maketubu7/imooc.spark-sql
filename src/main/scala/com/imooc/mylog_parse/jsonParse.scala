package com.imooc.mylog_parse

import org.apache.spark.sql.{DataFrame, SparkSession}

object jsonParse {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]")
      .appName("jsonParse")
      .getOrCreate()


    val jsonDF: DataFrame = spark.read.format("json").load("E:\\idea_workspace\\Spark_code_hive\\datas\\class_int.json")

    jsonDF.createOrReplaceTempView("k2")

    //spark.sql("select cname,floor,explode(student.age),explode(student.name),explode(student.score) from k2").show(false)
    spark.sql("select cname,floor,explode(student.age) from k2").show(false)
  }

}
