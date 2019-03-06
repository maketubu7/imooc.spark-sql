package spark

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import spark_sql.SparkUtil

object BroadcaseWC {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtil.createSparkContext(true,"BroadcaseWC")

    val rdd = sc.textFile("datas/little_king.txt").persist(StorageLevel.MEMORY_ONLY)
    //prince's  ("watch,1),(planets--,1)
  /*  val res = rdd.flatMap(_.split(" "))
      .map(x =>{(x,1)})
      .reduceByKey(_ + _)*/

    //沒有了("watch,1),(planets--,1)
    val list = List(",","'","\"","--","!",".",",",":",";","-")
    //广播变量list，到每个executor
    val broadcastlist: Broadcast[List[String]] = sc.broadcast(list)
    /* val res = rdd.flatMap(_.split(" "))
    .filter(x => {x.trim.nonEmpty && !list.exists(y =>{x.contains(y)})})
    .map(x =>(x,1)).reduceByKey(_ + _)
    .coalesce(1).map(_.swap)
    .sortByKey().map(_.swap)*/



    val res = rdd.flatMap(_.split(" ")).filter(_.trim.nonEmpty)
      .map(word =>{
        //foldLeft下面这个代码  为对传入的list  做一个替换，
        // 替换第一个的结果继续替换下一个，这个replace就相当于op运算符
        (broadcastlist.value.foldLeft(word)((words,ch) => words.replace(ch,"")),1)
        //这里运用了广播变量，让这个每个task都要用到数据，在每个executor中存一个副本
        //不用每次运行task的时候，都到driver上去拉数据，直接在executor中拿数据
      }).reduceByKey(_ + _)



    res.foreach(println)

  }

}
