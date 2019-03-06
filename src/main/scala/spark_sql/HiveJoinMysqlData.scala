package spark_sql

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object HiveJoinMysqlData {
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

    //1 hive读取数据写入mysql
    sk.sqlContext.read.table("dept")
      .write
      .mode("overwrite")
      .jdbc(url,"hive_dept",props)
    //hive当中的表，根本不需要单独读出来做为临时表，因为可以在写sql的时候直接获取到


    sk.sqlContext.read
      //.jdbc(url,"hive_dept",Array[String]("deptno < 25","deptno >=25 and deptno <28","deptno>=28"),props)
      //让每一个task根据自己所获得的条件去mysql中获取分区的数据
      //分区数和并行度不能过高，mysql设置连接数=200

   /* url: String,
    table: String,
    columnName: String,
    lowerBound: Long,
    upperBound: Long,
    numPartitions: Int,
    connectionProperties: Properties*/

      .jdbc(url,"hive_dept","deptno",10,40,5,props)
      .createOrReplaceTempView("mysql_dept")


    val joinedDF: DataFrame = sk.sqlContext.sql(
      """
        |SELECT a.*
        |FROM emp a
        |INNER JOIN mysql_dept b
        |ON a.deptno = b.deptno
      """.stripMargin)

    joinedDF.cache()
    joinedDF.show()

    joinedDF.write
      .mode(SaveMode.Overwrite)
      //在2.2.0之后的版本可以使用.format("hive")
      .format("hive")
      .saveAsTable("resjoin")

    Thread.sleep(1000000l)
    joinedDF.unpersist()


  }

}
