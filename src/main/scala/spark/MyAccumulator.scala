package spark


import org.apache.spark.{AccumulableParam, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable
import scala.collection.mutable._
/**
  * Created by ibf on 2018/7/25.
  */
//是否是因为Map要使用mutable
//import scala.collection.mutable._ 要事先导入
// 否则实现的是不可变的Map，就会报错，XXXX to val
object MyMapAccumulable extends AccumulableParam[Map[String,Int],String]{
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
}

class MyMapAccumulable2 extends AccumulatorV2[String,Map[String,Int]]{

  private val _mymap = mutable.Map[String, Int]()

  override def isZero: Boolean = {
    true
  }

  override def reset(): Unit = {
    this._mymap.clear()
  }
  override def add(v: String): Unit = {
    this._mymap += v -> (1 + this._mymap.getOrElse(v,0))
  }
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    other match {
      case o: MyMapAccumulable2 => (this._mymap /: o._mymap)
      {
        case (map, (k,v)) => map += ( k -> (v + map.getOrElse(k, 0)) )
      }
    }
  }
  override def value: mutable.Map[String, Int] = {
    this._mymap
  }
  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    var newacc = new MyMapAccumulable2
    newacc = this
    newacc
  }
}



object Accumulator {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Accumulator")
      .setMaster("local[2]")

    val sc = SparkContext.getOrCreate(conf)
    val rdd: RDD[String] = sc.parallelize(Array(
      "hadoop,spark,hbase",
      "hadoop,spark,hbase",
      "hhhh,spark,hbase",
      "hadoop,spark,hbase",
      "hadoop,hive,a",
      null,
      "hadoop,spark,ttt",
      "y,spark,hbase"
    ),1)

    //自定义累加器
    val newacc = new MyMapAccumulable2
    sc.register(newacc,"myacc")

    //系统累加器
    val newacc3 = sc.longAccumulator("myacc3")

    val myMapAccumulable = sc.accumulable(Map[String,Int]())(MyMapAccumulable)
    rdd.foreachPartition(iter =>{
      iter.foreach(line =>{
        val nline = if(line == null)"" else line
        nline.split(",")
          .filter(_.trim.nonEmpty)
          .map(word =>{
            //println(word + "dasdas")
            //myMapAccumulable += word
            newacc.add(word)
            println(newacc.value + "cvbc")
           // newacc3.add(1)
          })
        })
      })
   //测试
    val newacc2 = new MyMapAccumulable2
    sc.register(newacc2,"myacc2")

    newacc2.add("hadoop")
    newacc2.add("hadoop")
    newacc2.add("hive")
    newacc2.add("hadoop")
    newacc2.add("hadoop")
    newacc2.add("hive")

    println("myMapAccumulable:" + myMapAccumulable.value.mkString("[",",","]"))
    println("newacc2:" + newacc2.value.mkString("[",",","]"))
    println("newacc:" + newacc.value)
    newacc.merge(newacc2)
    println("newacc:" + newacc.value)

    //println("newacc:" + newacc.value)

   // println(newacc3.value)
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
