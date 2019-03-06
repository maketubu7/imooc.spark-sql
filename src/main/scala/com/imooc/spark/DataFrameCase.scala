package com.imooc.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * DataFrame中的操作操作
  * 文本数据转DF数据
 */
object DataFrameCase {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

    // RDD ==> DataFrame
    val rdd = spark.sparkContext.textFile("file:///Users/rocky/data/student.data")

    //注意：需要导入隐式转换
    import spark.implicits._
    val studentDF = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()

    //show默认只显示前20条
    studentDF.show
    studentDF.show(30)
    studentDF.show(30, false)

    studentDF.take(10)
    studentDF.first()
    studentDF.head(3)


    studentDF.select("email").show(30,false)


    studentDF.filter("name=''").show
    studentDF.filter("name='' OR name='NULL'").show
    studentDF.filter("name != '' and name != 'null'").show(23,false)


    //name以M开头的人
    studentDF.filter("SUBSTR(name,0,1)='M'").show

    studentDF.sort(studentDF("name")).show
    studentDF.sort(studentDF("name").desc).show

    studentDF.sort("name","id").show
    studentDF.sort(studentDF("name").asc, studentDF("id").desc).show

    studentDF.select(studentDF("name").as("student_name")).show


    val studentDF2 = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()

    studentDF.join(studentDF2, studentDF.col("id") === studentDF2.col("id")).show

    //分割线
    val rdd1 = spark.sparkContext.textFile("/opt/cdh/datas/student.txt")
    //分隔符 要用转义符进行转义，不然解析出来的是错误的
    val res = rdd1.map(_.split("\\|")).map(line =>{Student(line(0).toInt,line(1),line(2),line(3))}).toDF()

    res.show()

    res.filter("name == '' or name == 'null'").show

    res.select(res.col("name"),res.col("email")).show(30,false)
    //按照name的升序，id的降序进行排序
    res.sort(res.col("name").asc,res.col("id").desc).show
    //对结果重命名
    res.select(res.col("name").as("name-rename")).show

    res.join(studentDF2,res.col("id") === studentDF2.col("id")).show(30,false)






    spark.stop()
  }

  case class Student(id: Int, name: String, phone: String, email: String)

  //分割线


}
