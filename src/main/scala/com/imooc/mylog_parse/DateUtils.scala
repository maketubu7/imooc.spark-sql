package com.imooc.mylog_parse





import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期转换工具类
  */
object DateUtils {

  //[10/Nov/2016:00:01:02 +0800]
  val default_format_ddMMMyyyy = FastDateFormat.getInstance("dd/MMM/yyyy:hh:mm:ss Z",Locale.ENGLISH)

  val standerd_format_yyyymmdd = FastDateFormat.getInstance("yyyy-MM-dd hh:mm:ss")


  //结合gettime 转换为标准格式
  def time_parse(time: String) ={
    standerd_format_yyyymmdd.format(new Date(gettime(time)))
  }


  //转换为时间戳格式
  def gettime(time: String) = {
    val restime = time.substring(time.indexOf("[") + 1,time.lastIndexOf("]"))

    default_format_ddMMMyyyy.parse(restime).getTime
  }


}
