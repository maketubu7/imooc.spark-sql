package spark_stream

import java.text.SimpleDateFormat

import Utils.SparkUtil
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Time}

object DStream2RddAPI {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtil.createSparkContext(true,"DStream2RddAPI")

    val ssc = new StreamingContext(sc,Milliseconds(2000))
    //dstream 当中有一些api是没有的（例如：sortbyKey等）
    //将DStream转换成RDD进行操作,可以使用多个API
    val DStream: ReceiverInputDStream[String] = ssc.socketTextStream("make.spark.com",9999)
    val dateformate = new SimpleDateFormat("yyyyMMdd HH:mm:ss")

    val newDStream: DStream[((String, String), Int)] = DStream.transform((rdd, timestamp) => {
      val time: String = dateformate.format(timestamp.milliseconds)
      //自己的业务逻辑代码
      rdd.flatMap(_.split(" "))
        .filter(_.nonEmpty)
        .map(k => ((k,time),1))
        .reduceByKey(_ + _)
        //传入ascending参数倒序排序
        .sortBy(t =>t._2,ascending = false)
    })

    newDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
