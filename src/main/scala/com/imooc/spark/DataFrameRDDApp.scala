package com.imooc.spark

import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * DataFrame和RDD的互操作
 */
object DataFrameRDDApp {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

    //inferReflection(spark)

    program(spark)

    spark.stop()
  }

  def program(spark: SparkSession): Unit = {
    // RDD ==> DataFrame
    //编程转换，
    val rdd = spark.sparkContext.textFile("/opt/cdh/datas/student.data")

    val infoRDD = rdd.map(_.split(",")).map(line => Row(line(0).toInt, line(1), line(2).toInt))

    val structType = StructType(Array(StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)))

    val infoDF = spark.createDataFrame(infoRDD,structType)
    infoDF.printSchema()
    infoDF.show()


    //通过df的api进行操作
    infoDF.filter(infoDF.col("age") > 30).show

    //通过sql的方式进行操作
    infoDF.createOrReplaceTempView("infos")
    spark.sql("select * from infos where age > 30").show()
  }

  def inferReflection(spark: SparkSession) {
    // RDD ==> DataFrame
    val rdd = spark.sparkContext.textFile("file:///Users/rocky/data/infos.txt")

    //注意：需要导入隐式转换
    import spark.implicits._
    val studentDF = rdd.map(_.split(",")).map(line => student(line(0).toInt, line(1), line(2).toInt)).toDF()

    studentDF.show()
    studentDF.filter("substr(name,0,1) = 'T'").show()

    studentDF.filter(studentDF.col("age") > 30).show

    studentDF.createOrReplaceTempView("student")

    spark.sql("select * from infos where age > 30").show()
  }

  case class student(id: Int, name: String, age: Int)

}
