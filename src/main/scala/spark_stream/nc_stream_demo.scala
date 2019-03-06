package spark_stream

import Utils.SparkUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object nc_stream_demo {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtil.createSparkContext(true,"nc_stream")
    //创建streamContext上下文
    val ssc: StreamingContext = new StreamingContext(sc,Seconds(5))

    //创建数据流方式
    val line: ReceiverInputDStream[String] = ssc.socketTextStream("make.spark.com",9999)


    //处理数据流
    val res = line.flatMap(_.split(" "))
      .map(t => (t,1))
        .reduceByKey(_ + _)


    res.print()

    //启动数据流
    ssc.start()
    //让数据流一直堵塞，保持持续运行
    ssc.awaitTermination()

    //关闭stream程序

 /*   var i= 0

    while (i < 10){
      i = i + 1
      Thread.sleep(2000)
      println(s"当前的i为：${i}")

      if(i == 10){
        ssc.stop(true,true)
      }
    }*/
  }
}
