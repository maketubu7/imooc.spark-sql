package com.imooc.mylog_parse

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

object StatDao {

  def delete_dateinfo (day:String): Unit ={
    var conn:Connection = null
    var pstm:PreparedStatement = null

    try{

      conn = MySqlUtils.getConnection()

      conn.setAutoCommit(false)

      val tables = Array("day_video_topn_stat","day_traffics_video_topn_stat"
      ,"day_city_video_topn_stat")

      for (table <- tables){
        //println(table)
        val deletesql = s"delete from ${table} where day = ?"

        pstm = conn.prepareStatement(deletesql)
        pstm.setString(1,day)
        pstm.execute()
        conn.commit()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySqlUtils.release(conn, pstm)
      println(s"已删除$day" + "的相关数据")
    }

  }
  def DayToopN_insert (list: ListBuffer[DayVideoAccessStat]): Unit ={

    var conn:Connection = null
    var pstm:PreparedStatement = null

    try{

      conn = MySqlUtils.getConnection()

      //设置为手动提交
      conn.setAutoCommit(false)

      val sql = "insert into day_video_topn_stat(day, immocid, times)" +
        " values (?,?,?)"

      pstm = conn.prepareStatement(sql)

      for (ele <- list){
        pstm.setString(1,ele.day)
        pstm.setLong(2,ele.immocid)
        pstm.setLong(3,ele.times)

        //先添加到批次里

        pstm.addBatch()
      }

      //批量执行
      pstm.executeBatch()
      //手动提交
      conn.commit()

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySqlUtils.release(conn, pstm)
    }
  }

  def CityTopN_insert (list: ListBuffer[CityVideoAccessStat]): Unit ={

    var conn:Connection = null
    var pstm:PreparedStatement = null

    try{

      conn = MySqlUtils.getConnection()

      //设置为手动提交
      conn.setAutoCommit(false)

      val sql = "insert into day_city_video_topn_stat(day, immocid, city, times, times_rnk)" +
        " values (?,?,?,?,?)"

      pstm = conn.prepareStatement(sql)

      for (ele <- list){
        pstm.setString(1,ele.day)
        pstm.setLong(2,ele.immocid)
        pstm.setString(3,ele.city)
        pstm.setLong(4,ele.times)
        pstm.setInt(5,ele.times_rnk)

        //先添加到批次里
        pstm.addBatch()
      }

      //批量执行
      pstm.executeBatch()
      //手动提交
      conn.commit()

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySqlUtils.release(conn, pstm)
    }
  }

  def TrafficTopN_insert (list: ListBuffer[TrafficVideoAccessStat]): Unit ={

    var conn:Connection = null
    var pstm:PreparedStatement = null

    try{

      conn = MySqlUtils.getConnection()

      //设置为手动提交
      conn.setAutoCommit(false)

      val sql = "insert into day_traffics_video_topn_stat (day, immocid, traffics)" +
        " values (?,?,?)"

      pstm = conn.prepareStatement(sql)

      for (ele <- list){
        pstm.setString(1,ele.day)
        pstm.setLong(2,ele.immocid)
        pstm.setLong(3,ele.traffics)

        //先添加到批次里
        pstm.addBatch()
      }

      //批量执行
      pstm.executeBatch()
      //手动提交
      conn.commit()

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySqlUtils.release(conn, pstm)
    }
  }

}
