package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object mywordcount {
  def main(args: Array[String]) {
    //1/创建sparkconf和spark上下文
    //spark.mywordcount
    /**
      *
    A master URL must be set in your configuration
        An application name must be set in your configuration
        所有的配置文件信息，其实都是在sparkconf当中加载的，所以如果你要设置
        配置文件的信息的话，conf.set("key","value")
      */
    val conf = new SparkConf()
    //本地模式，* 会在运行期间检查当前环境下还剩下多少cpu核心，占满
    //.setMaster("local[*]")
    //.setAppName("idea_start_wc")

    val sc = new SparkContext(conf)

    val coalesceNum = Integer.parseInt(conf.get(args(0)))
    //以REST API提交的参数设置
    //val coalesceNum = Integer.parseInt(args(0))

    val resultRdd: RDD[(String, Int)] = sc.textFile("hdfs://192.168.89.77:8020/spark/datas/test.txt")
      .flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _) //到这一步已经实现wc
      .map(t=>(-t._2,t._1)).sortByKey().map(t=>(t._2,-t._1)) //这一步做排序
      //repartition
      //coalesce
      //以上这两个重分区的api有什么区别？
      .coalesce(coalesceNum)

    //保存
    resultRdd.saveAsTextFile(s"hdfs://192.168.89.77:8020/spark/sparkrdd_idea/wordcount_res${System.currentTimeMillis()}")

    //调用线程等待，为了方便去页面上看结果信息
    //程序终止（通过正常手段关闭程序）
    sc.stop()

  }
}
