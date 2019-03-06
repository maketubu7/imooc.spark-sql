package com.imooc.mylog_parse

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
/**
  *日志解析工具类
 */
object log_parseutil {

  //2017-05-11 14:09:14	http://www.imooc.com/video/4500	304	218.75.35.226
  //time url traffic ip
  val schema = StructType(
    Array(StructField("url",StringType),
      StructField("immoctype",StringType),
      StructField("immocid",LongType),
      StructField("traffic",LongType),
      StructField("ip",StringType),
      StructField("city",StringType),
      StructField("time",StringType),
      StructField("day",StringType)
    )
  )


    def logparse(log: String): Row ={

      try{
        val splits = log.split("\t")

        val url = splits(1)
        val time = splits(0)
        val ip = splits(3)
        val traffic = splits(2).toLong

        val domain = "http://www.imooc.com/"
        val immoc = url.substring(url.indexOf(domain) + domain.length).split("/")

        var immoctype = ""
        var immocid = 0L
        if (immoc.length ==2){
          immoctype = immoc(0)
          immocid = immoc(1).toLong
        }

        val city = Ip_Uitls.getCity(ip)
        val day = time.substring(0,10).replaceAll("-","")

        Row(url, immoctype, immocid, traffic, ip, city, time, day)

      } catch {
        case e:Exception => Row(0)
      }

    }
}
