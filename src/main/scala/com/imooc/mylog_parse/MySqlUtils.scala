package com.imooc.mylog_parse

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * mysql 的工具类
  */
object MySqlUtils {

  //得到一个mysql数据库连接


  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/immoc_project?user=root&password=123456")
  }

  //释放得到的资源 conn pstm
  def release (conn:Connection,pstm:PreparedStatement): Unit ={
    try{
      if(pstm != null){
        pstm.close()
      }
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      if (conn != null){
        conn.close()
      }
    }
  }

/*
    def main(args: Array[String]): Unit = {
      println(getConnection())
  }
  */
}
