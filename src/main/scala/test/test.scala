package test

import org.apache.spark.sql.{DataFrame, SparkSession}

object test {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

//    groupby

    val res: DataFrame = spark.read.format("parquet").load("datas/immoc/output/clean_log_job_02/")

    res.show(false)

  }

  private def groupby = {
    val a: Iterable[((String, Int), Int)] = null

    val b: ((String, Int), Int) = (("xx", 1), 1)
    val b1: ((String, Int), Int) = (("ww", 1), 1)
    val b2: ((String, Int), Int) = (("qq", 1), 1)
    val b3: ((String, Int), Int) = (("zz", 1), 1)

    val c: Iterable[((String, Int), Int)] = Iterable(b, b1, b2, b3)

    val res: Map[(String, Int), Iterable[((String, Int), Int)]] = c.groupBy(_._1)

    res.foreach(println)
  }
}

