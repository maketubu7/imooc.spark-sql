package spark_test

import scala.util.Random

object second_aggregation {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtil.createSparkContext(true,"second_aggregation")

    val rdd = sc.textFile("datas/second_aggregation.txt")
    //val prifix=new Random().nextInt(10)
    val checkpointpath = "datas/checkpoint"
    sc.setCheckpointDir(checkpointpath)

    val res1 =rdd.flatMap(_.split(" "))
       .map(x => {(x,1)})
      .map(x => {(Random.nextInt(10)+"-"+x._1,x._2)})
      .reduceByKey(_ + _,12)

      res1.cache()

      res1.count()

     val res2 = res1.map(x =>(x._1.split("-")(1),x._2))
       .reduceByKey(_ + _,2)

    res2.cache

      res1.foreach(println)
      println("==================")
      res2.foreach(println)

    Thread.sleep(2000000)
  }

}
