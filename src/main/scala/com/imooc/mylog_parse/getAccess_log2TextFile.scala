package com.imooc.mylog_parse

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object getAccess_log2TextFile {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]")
//      .config("spark.sql.orc.compression.codec","gzip")
      .config("spark.sql.parquet.compression.codec","gzip")
      .appName("getAccess_log2TextFile")
      .getOrCreate()
    //2017-05-11 14:09:14	http://www.imooc.com/video/14540	304	218.75.35.226
    //time url traffic ip
    val logrdd: RDD[String] = spark.sparkContext.textFile("datas/immoc/input/access.log")
    //这里创建一个日志 解析工具类

    val logDf: DataFrame = spark.createDataFrame(logrdd.map(line => log_parseutil.logparse(line)),
      log_parseutil.schema)

//    logDf.printSchema()
//    logDf.show(false)

    //分区为1    存储格式为parquet   按day这个字段进行分区   保存路径、
    logDf.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
      .partitionBy("day").save("datas/immoc/output/clean_log_job/")

    spark.close()

  }

}
