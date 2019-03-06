package com.imooc.mylog_parse

import com.ggstar.util.ip.IpHelper
object Ip_Uitls {

//  def main(args: Array[String]): Unit = {
//    println(getcity("172.168.89.77"))
//  }
  def getCity(ip:String) ={
    IpHelper.findRegionByIp(ip)
  }
}
