package com.imooc.mylog_parse

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object getAccess_log2TextFile {
  def main(args: Array[String]): Unit = {

    if (args.length != 2){
      println("agment error: <inputpath> <outputpath>")
      System.exit(1)
    }

    val Array(inpath,outpath) = args

    val spark = SparkSession.builder().getOrCreate()


    val logrdd: RDD[String] = spark.sparkContext.textFile(inpath)

    val logDf: DataFrame = spark.createDataFrame(logrdd.map(line => log_parseutil.logparse(line)),
      log_parseutil.schema)

    logDf.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
      .partitionBy("day").save(outpath)

    spark.close()

  }

}
