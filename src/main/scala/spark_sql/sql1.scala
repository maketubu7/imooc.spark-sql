package spark_sql

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.CreateHiveTableContext
import org.apache.spark.{SparkConf, SparkContext}

object sql1 {
  def main(args: Array[String]): Unit = {
    val sparkSession : SparkSession = SparkSession.builder.
      master("local[*]")
      .appName("sql")
      .getOrCreate


    sparkSession.sql("select 6/7").show()


  }
}
