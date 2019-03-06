package spark_sql

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object jdbc_test {
  def main(args: Array[String]): Unit = {
    lazy val driver = "com.mysql.jdbc.Driver"
    lazy val url = "jdbc:mysql://make.spark.com:3306/test"
    lazy val user = "root"
    lazy val password = "123456"
    lazy val props = new Properties()
    props.put("user",user)
    props.put("password",password)

    val conf = new SparkConf()
      .setAppName("csv")
      .setMaster("local[*]")
      .set("spark.sql.shuffle.partitions","10")
    val sc = SparkContext.getOrCreate(conf)

    val sk = SparkSession.builder()
      .master("local[*]")
      .appName("csv")
      .enableHiveSupport()
      .getOrCreate()

    //一个分区对应一个job
    //两个分区对应两个job  1/1 1/1
    //4个分区对应2个job    1/1 3/3
   // sk.sqlContext.read.jdbc(url,"hive_dept","deptno",10,40,1,props).show()
    sk.sqlContext.read
      .jdbc(url,"hive_dept",Array("deptno < 25","deptno >=25 AND deptno < 28","deptno >=28"),props)
      .show()

    Thread.sleep(2000000)

  }

}
