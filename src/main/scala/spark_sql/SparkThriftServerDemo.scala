package spark_sql

import java.sql.{Connection, PreparedStatement, ResultSet, DriverManager}

/**
  * Created by ibf on 2018/7/27.
  */
object SparkThriftServerDemo extends  App{
  //1 driver
  val driver = "org.apache.hive.jdbc.HiveDriver"
  //加载driver
  Class.forName(driver)

  val (url,user,password) = ("jdbc:hive2://make.spark.com:10000","make","123456")
  //获取conn
  val conn: Connection = DriverManager.getConnection(url,user,password)
  //预编译sql
  val sql =
    """select a.*
      |from
      |make.emp a
      |inner join
      |make.dept b
      |on a.deptno = b.deptno """.stripMargin

  val pstmt: PreparedStatement = conn.prepareStatement(sql)

  val rs: ResultSet = pstmt.executeQuery()

  while (rs.next()){
    println(rs.getString("ename")+"-------"+rs.getDouble("sal"))
  }

  /*val sql2 =
    """
      |select
      |deptno,avg(sal) as avg_sal
      |from
      |make.emp a
      |group by deptno
      |having avg_sal > ?
    """.stripMargin

  val pstmt2 =  conn.prepareStatement(sql2)
  pstmt2.setInt(1,2000)
  val rs2 = pstmt2.executeQuery()
  while (rs2.next()){
    println(rs2.getString("deptno")+"-------"+rs2.getDouble("avg_sal"))
  }*/

}

