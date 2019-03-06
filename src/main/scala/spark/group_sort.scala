package spark

import spark_sql.SparkUtil

import scala.collection.JavaConverters._
import scala.util.Random

/**
  * xxxbykey是pairrdd才能进行的操作
  */
object group_sort {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtil.createSparkContext(true,"group_sort")
    val nums = 3
    val path = "datas/groupsort.txt"
    val rdd = sc.textFile(path)
    //因为我们要的是每一行的数据为一个整体，第一个算子应该用map
   /*
      (aa,List(78, 80, 97))
      (bb,List(92, 97, 98))
      (cc,List(86, 87, 98))
  */
   /* rdd.map(_.split(" "))
      .filter(t => (t.length>=2))
      .map(t => (t(0),t(1).toInt))
      .groupByKey().map(
      {
        case (key,itr) =>{
          //因为内部要进行排序，所以转换为list进行内部排序,为升序排序，取最后nums个
          (key,itr.toList.sorted.takeRight(nums))
        }
      }
    ).foreach(println)
*/
    /*rdd.map(_.split(" "))
      .filter(t => (t.length>=2))
      .map(t => (Random.nextInt(10)+"-"+ t(0),t(1).toInt))
   /* 用map进行计算，返回为一个集合，flatmap，可以将集合进行展开
      List((aa,78), (aa,80), (aa,97))
      List((bb,92), (bb,97), (bb,98))
      List((cc,86), (cc,87), (cc,98))
    */
      .groupByKey().flatMap(
      {
        case (key,itr) => {
          //因为内部要进行排序，所以转换为list进行内部排序,为升序排序，取最后nums个
          val items = itr.toList.sorted.takeRight(nums)
          items.map(it => (key, it))
        }
      }
    ).foreach(println)*/

    //局部排序后，再全局排序，但是有可能有问题，保证排序的后take的个数为一样的，就没问题
    rdd.map(_.split(" "))
      .filter(t => (t.length>=2))
      .map(t => (Random.nextInt(10)+"-"+ t(0),t(1).toInt))
      .groupByKey().flatMap(
      {
        case (key,itr) => {
          //因为内部要进行排序，所以转换为list进行内部排序,为升序排序，取最后nums个
          val items = itr.toList.sorted.takeRight(nums)
          items.map(it => (key, it))
        }
      }
    )
      .map(t =>(t._1.split("-")(1),t._2)).groupByKey().flatMap(
      {
        case (key,itr) => {
          //因为内部要进行排序，所以转换为list进行内部排序,为升序排序，取最后nums个
          val items = itr.toList.sorted.takeRight(nums)
          items.map(it => (key, it))
        }
      }
    ).foreach(println)

  }
}
