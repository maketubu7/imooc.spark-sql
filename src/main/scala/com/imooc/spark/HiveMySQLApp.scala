package com.imooc.spark

import org.apache.spark.sql.SparkSession

/**
 * 使用外部数据源综合查询Hive和MySQL的表数据
 */
object HiveMySQLApp {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("HiveMySQLApp")
      .master("local[2]").getOrCreate()

    // 加载Hive表数据
    val hiveDF = spark.table("emp")

    // 加载MySQL表数据
    val mysqlDF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306")
      .option("dbtable", "spark.DEPT")
      .option("user", "root")
      .option("password", "root")
      //添加jar包到jars 文件夹下，不然会报class not found 的错误
      .option("driver", "com.mysql.jdbc.Driver")
      .load()

    // JOIN
    val resultDF = hiveDF.join(mysqlDF, hiveDF.col("deptno") === mysqlDF.col("DEPTNO"))
    resultDF.show


    resultDF.select(hiveDF.col("empno"),hiveDF.col("ename"),
      mysqlDF.col("deptno"), mysqlDF.col("dname")).show


    //分割线

    val hiveDF2 = spark.table("emp")
    import java.util.Properties
    val pop = new Properties()

    pop.put("user", "root")
    pop.put("password", "123456")
    pop.put("driver", "com.mysql.jdbc.Driver")


    val mysqlDF2 = spark.read.jdbc("jdbc:mysql://localhost:3306","emp",pop)

    hiveDF2.join(mysqlDF2,hiveDF2.col("deptno") === mysqlDF2.col("DEPTNO")).show()

    val res = hiveDF2.join(mysqlDF2,hiveDF2.col("deptno") === mysqlDF2.col("DEPTNO"))

    //保存结果到本地csv文件
    val result = res.select("empno","ename","sal").write.format("csv")
      .option("header","true").option("inferSchema","true")
      .save("/opt/cdh/datas/test.csv")

    spark.stop()
  }

}
