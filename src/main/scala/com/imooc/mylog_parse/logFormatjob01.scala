package com.imooc.mylog_parse

import org.apache.spark.sql.SparkSession

/**
  * 从原始数据提出想要的数据
  */

object logFormatjob01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]")
      .appName("logFormatjob01")
      .getOrCreate()

    val prelog = spark.sparkContext.textFile("datas/immoc/input/10000access.log")

    val reslog = prelog.map(line => {
      val splits = line.split(" ")
      val ip = splits(0)

      /**
        * 原始日志的第三个和第四个字段拼接起来就是完整的访问时间：
        * [10/Nov/2016:00:01:02 +0800] ==> yyyy-MM-dd HH:mm:ss
        */
      val time = splits(3) + " " + splits(4)
      val url = splits(11).replaceAll("\"", "")
      val traffic = splits(9)
      //      (ip, DateUtils.parse(time), url, traffic)
      DateUtils.time_parse(time) + "\t" + url + "\t" + traffic + "\t" + ip
    }).saveAsTextFile("datas/immoc/output/log_job_01/")


  }
}
