package com.imooc.mylog_parse

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import scala.collection.mutable.ListBuffer

/**
  * 需求一：每天最受欢迎的课程topnN
  * 需求二：每个城市的每天最受欢迎的课程topN
  * 需求二：每天按流量计算最受欢迎的课程topN
  */
object AccessLogTopNJob {




  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]")
      //关闭自动识别字段类型功能
      .config("spark.sql.sources.partitionColumnTypeInference.enabled","false")
      .appName("AccessLogTopNJob")
      .getOrCreate()

    val videoDF = spark.read.format("orc").load("datas/immoc/output/clean_log_job")

    //对当天的数据进行删除


    StatDao.delete_dateinfo(day)

    //按照时间统计最受欢迎topN课程
    videoaccesslogTopN(spark,videoDF, day)

    //按照地市进行统计最受欢迎topN课程
    cityaccesslogTopN(spark,videoDF, day)

    //按照流量统计最受欢迎topN课程
    trafficaccesslogTopN(spark,videoDF, day)

  }

  val day = "20170511"

  /**
    * 最受欢迎的课程topN
    * @param spark
    * @param videoDF
    * @param day
    */


  def videoaccesslogTopN(spark: SparkSession, videoDF: DataFrame, day:String): Unit = {

    /**
      * 1、DF方式实现video topN的统计
      */
    import spark.implicits._
    val videoaccseeDF = videoDF.filter($"day" === day && $"immoctype" === "video")    //筛选
      .groupBy("day","immocid")   //分组
      .agg(count("immocid").as("times"))    //count计数
      .orderBy($"times".desc)   //top排序

    videoaccseeDF.printSchema()
    videoaccseeDF.show(10)

    /**
      * 2、sql方式实现video topN的统计
      */
//    videoDF.createOrReplaceTempView("videoaccess_log")
//    val day_value = "20170511"
//    val immoctype_value = "video"
//    val videoaccessDF = spark.sql("select day,immocid,count(1) as times from videoaccess_log " +
//      "where day = day_value and immoctype = immoctype_value " +
//      "group by day,immocid " +
//      "order by times desc ")
//
//    videoaccessDF.show(10)
    /**
      * 统计结果写到mysql数据库中
      */
    try {
      val list = new ListBuffer[DayVideoAccessStat]

      videoaccseeDF.foreachPartition(PartialRecodd =>{

        PartialRecodd.foreach(line => {
          val day = line.getAs[String]("day")
          val immocid = line.getAs[Long]("immocid")
          val times = line.getAs[Long]("times")

          list.append(DayVideoAccessStat(day, immocid, times))
        })

        StatDao.DayToopN_insert(list)
      })
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }

  /**
    * 每个城市的最受欢迎的课程top3
    * @param spark
    * @param videoDF
    * @param day
    */
  def cityaccesslogTopN(spark: SparkSession, videoDF: DataFrame, day:String) = {
    import spark.implicits._
    val videoaccseeDF = videoDF.filter($"day" === day && $"immoctype" === "video")
      .groupBy("day","immocid","city").agg(count("immocid").as("times")).orderBy($"immocid".desc,$"times".desc)

//    videoaccseeDF.printSchema()
//    videoaccseeDF.show(50)

    val cityaccseeDF = videoaccseeDF.select($"day", $"city", $"immocid", $"times",
      row_number().over(Window.partitionBy($"city").orderBy($"times".desc)).as("times_rnk")
    ).filter($"times_rnk" < 4)


    cityaccseeDF.show(30)

    try {
      val list = new ListBuffer[CityVideoAccessStat]

      cityaccseeDF.foreachPartition(PartialRecodd =>{

        PartialRecodd.foreach(line => {
          val day = line.getAs[String]("day")
          val immocid = line.getAs[Long]("immocid")
          val city = line.getAs[String]("city")
          val times = line.getAs[Long]("times")
          val times_rnk = line.getAs[Int]("times_rnk")

          list.append(CityVideoAccessStat(day, immocid, city, times, times_rnk))
        })

        StatDao.CityTopN_insert(list)
      })
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }

  /**
    * 每天按流量的最受欢迎的课程topN
    * @param spark
    * @param videoDF
    * @param day
    */
  def trafficaccesslogTopN(spark: SparkSession, videoDF: DataFrame,day:String)  = {
    import spark.implicits._
    val videoaccseeDF = videoDF.filter($"day" === day && $"immoctype" === "video")
      .groupBy("day","immocid").agg(sum($"traffic").as("traffics"))
      .orderBy($"traffics".desc)

    try {
      val list = new ListBuffer[TrafficVideoAccessStat]

      videoaccseeDF.foreachPartition(PartialRecodd =>{

        PartialRecodd.foreach(line => {
          val day = line.getAs[String]("day")
          val immocid = line.getAs[Long]("immocid")
          val traffics = line.getAs[Long]("traffics")

          list.append(TrafficVideoAccessStat(day, immocid, traffics))
        })

        StatDao.TrafficTopN_insert(list)
      })
    } catch {
      case e:Exception => e.printStackTrace()
    }

  }
}
