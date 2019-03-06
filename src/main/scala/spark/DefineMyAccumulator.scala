package spark

import org.apache.spark.AccumulableParam
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import spark_sql.SparkUtil

import scala.collection.mutable
import scala.collection.mutable._
/**
  * Created by ibf on 2018/7/25.
  */
//是否是因为Map要使用mutable
//import scala.collection.mutable._ 要事先导入
// 否则实现的是不可变的Map，就会报错，XXXX to val
/*object MyMapAccumulable extends AccumulableParam[Map[String,Int],String]{
  override def addAccumulator(r: Map[String, Int], t: String): Map[String, Int] = {
    //判断t是否存在r当中
    //如果存在，int+1，不存在，put进去，int=1
    r += t -> (1+r.getOrElse(t,0))

  }
  override def addInPlace(r1: Map[String, Int], r2: Map[String, Int]): Map[String, Int] = {
    //应该遍历r2集合，把r2集合中的相同字符串的数据int值相加
    //不相同的直接放入r1
    r1.foldLeft(r2)((ar2,br1) => {
      ar2 += br1._1 -> (ar2.getOrElse(br1._1,0)+br1._2)
    })
  }
  override def zero(initialValue: Map[String, Int]): Map[String, Int] = {
    initialValue
  }
}*/



object DefineMyAccumulator {
  def main(args: Array[String]) {
    //可以实现自己的累加器的类，来做避免groupby，distinct（shuffle）的操作
    val sc = SparkUtil.createSparkContext(true,"DefineMyAccumulator")

    var newacc1 = new MyMapAccumulable2
    sc.register(newacc1,"myacc1")
    val rdd: RDD[String] = sc.parallelize(List("hadoop","haha","hive","hbase"))
    //这句话一定要写全，否则会报错

    //rdd.foreach(t => println(t + "haha"))
    val l = List("hadoop","haha","hive","hbase")
    for (s <- l){
      println(s)
      newacc1.add(s)
    }
    var newacc2 = new MyMapAccumulable2
    sc.register(newacc2,"myacc")

    rdd.foreach(t => newacc2.add(t))
    println("newacc1" + newacc1.value.mkString("[",",","]"))
    println("newacc2" + newacc2.value.mkString("[",",","]"))


    //为什么这里进行了add，还是没有值
    //rdd.foreach(t => {newacc1.add(t)})



    /**
      * 表1  网页信息，用户访问了哪个页面
      *
      * 表2 网页信息  你想要看到的相关页面的指标分析
      *
      * 1000W     1W
      * uv pv   spark推荐不要使用太多的自定义对象，string1 ~ string2
      * join  表1网页信息  = 表2网页信息
      */

  }
}

