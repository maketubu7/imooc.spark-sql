package pvuv

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object sparkPVUV_standalone {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark Pi").setMaster("spark://make.spark.com:7070")

    val sc = new SparkContext(conf)

    //    val path = "hdfs://make.spark.com:8020/spark/datas/page_views.data"
    val path = "hdfs://spark/datas/page_views.data"
    //val checkpointpath = "datas/checkpoint"
    //sc.setCheckpointDir(checkpointpath)    //设置checkpoint的缓存DIR
    val originalRdd: RDD[String] = sc.textFile(path)
    //因为缓存不是立即操作的api，只有当调用了这块缓存的数据才会cache
    originalRdd.cache()
    originalRdd.count()
    //某些固定的值，应该要写在配置中，然后通过读取配置来获取
    val arrLen = 7
    val timeLen = 16


    //处理过后的rdd
    val mappedRdd: RDD[(String, String, String)] = originalRdd.map(_.split("\t"))
      .filter(arr =>{
        //保证7个字段          第一个时间字段大于16         第二个字段url长度大于0
        arr.length == arrLen && arr(0).trim.length > timeLen && arr(1).length > 0
      })
      //计算后转换格式为(date,url,guid)
      .map(arr =>{
      //每分钟的pv
      val date = arr(0).substring(0,16)
      val url = arr(1).trim
      val guid = arr(2).trim
      (date,url,guid)
    })

    mappedRdd.cache()
    mappedRdd.count()

    //计算PV
    /**
      * 其实计算pv只要维度（date）和url
      */
    //XXXByKey的操作是针对于PairRdd（二元组rdd）才能实现的，
    /*  val resultRdd = mappedRdd.map(t => (t._1,t._2))
        .groupByKey()
        .map{
        //date就是日期，itr是迭代器，里面把相同日期的value全部放到一起
        case (date,itr) =>{
          (date,itr.size)
        }
      }*/

    //思考：groupByKey这样的API，有没有什么其他API可以实现这个功能，他们之间的性能比较
    //这段代码有哪些地方是可以优化的
    /**
      * 1 优化： grouByKey 这个api性能不是特别好
      *      会把相同key的所有数据全部放到同一个迭代器中，数据倾斜
      *      API可以替换，
      *      是否可以不保留url的值，直接写1，然后用于后面的count
      */

    //def reduceByKey(func: (V, V) => V, numPartitions: Int)
    /**
      * 这里有一个numPartitions可以指定，分区数量
      * executor 5 个core  就可以并行计算5个分区的数据
      * 当数量大的时候，甚至出现数据倾斜的时候，可以通过增加分区数量来缓解每个task的计算压力
      *
      */
    val pvRdd = mappedRdd.map(t => (t._1,1))
      .reduceByKey(_ + _,5)

    pvRdd.cache()
    //pvRdd.checkpoint()
    pvRdd.count()

    /**
      * uv应该如何计算？count  distinct   groupby(XXX,xxx)
      * select count(distinct XXX) as uv from XX group by XXX
      * select count(1) from (select XXX from group by xxx ) tb
      * 在什么场景下应该用哪一种呢
      * key较为分散的情况下使用， key较为集中的情况下使用？
      */

    //方式一
    /* val uvRdd = mappedRdd.filter(t => t._3.nonEmpty)
      .map(t => {
      //把什么作为key然后进行聚合，每分钟的uv
      (t._1,t._3)
    }).groupByKey()
    .map({
      case (date,itr) =>{
        (date,itr.toSet.size)
      }
    })*/

    /**方法二：是否可以使用reduceByKey来做去重呢？
      * 我只想知道在同一个时间段内出现了多少key，key出现的次数，并不不关注
      * spark rdd的api的时候，要关注，你的key是什么？
      */
    //(date,url,uid)
    /*val uvRdd = mappedRdd.filter(t => t._3.nonEmpty)
        //((date,uid),1)
        .map(t => ((t._1,t._3),1))
        .reduceByKey({
      case (a,b) => a
    })
    //进行第二次聚合  ((13:01,uid1),1),((13:01,uid2),1),((13:01,uid3),1)
    //想要得到(13:01,3)
    .map({
      case ((date,uid),int) =>{
        (date,1)
      }
    }).reduceByKey(_ + _)
*/
    val uvRdd = mappedRdd.filter(t => t._3.nonEmpty)
      .map(t => (t._1,t._3))
      .distinct(10)     //10个分区
      .map(t =>(t._1,1))
      .reduceByKey(_ + _)

    uvRdd.cache()
    //uvRdd.checkpoint()
    uvRdd.count()

    //pvRdd.foreach(println)

    //uvRdd.foreach(println)
    //使用外联，计算出值的就保留值，没计算出来的就给定默认值-1
    /**
      * select date,
      * (case when pv is not null
      *  then pv
      *  else
      *  -1) as pv,
      *  (case when uv is not null
      *  then uv
      *  else
      *  -1) as uv from (select date,pv from A full join B on A.date = B.date) tb
      */

    val resultRdd: RDD[(String, Int, Int)] = pvRdd.fullOuterJoin(uvRdd)
      .map({
        case(date,(optpv,optuv)) =>{
          (date,optpv.getOrElse(-1),optuv.getOrElse(-1))
        }
      }).coalesce(1)
    uvRdd.unpersist()    //释放缓存
    resultRdd.foreach(println)
            resultRdd.saveAsTextFile(s"hdfs://192.168.89.77:8020/" +
             s"spark/sparkPVUV_${System.currentTimeMillis()}")

    Thread.sleep(100000000l)

    /*originalRdd.unpersist()
    mappedRdd.unpersist()
*/



  }

}
