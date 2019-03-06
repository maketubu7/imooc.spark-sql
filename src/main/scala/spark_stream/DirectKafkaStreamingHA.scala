package spark_stream

import Utils.{PropertiesUtil, PropertiesUtil_ka_str, SparkUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent


object DirectKafkaStreamingHA {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtil.createSparkContext(true,"DStream2RddAPI")

    val CheckpointPath = "datas/checkpoint/DStream2"

    def creatingFunc() = {

      val ssc = new StreamingContext(sc,Seconds(2))

      ssc.checkpoint(CheckpointPath)


      //设置相关参数
      val  topics: String = "producer_consumer_demo1"
      val brokers = PropertiesUtil_ka_str.getPropString("kafka.bootstrap.servers")

      //具体写法参照 http://spark.apache.org/docs/2.2.0/streaming-kafka-0-10-integration.html
      val topicarr = topics.split(",")
      val kafkaParams: Map[String, Object] = Map[String,Object](
        "bootstrap.servers" -> brokers,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> PropertiesUtil.getPropString("kafka.group.id"),
       // "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )

      val kafka_streamDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
        ssc,
        PreferConsistent,
        Subscribe[String,String](topicarr,kafkaParams))

      val resDStream: DStream[(String, Int)] = kafka_streamDStream
        //使用mapPartitions这样的api，在mapPartitions这个api中来定义数据的转换
        //来避免DAG图的改变
            .mapPartitions(iter =>{
        iter.map(k => k.value())
          .flatMap(_.split(" "))
          .map(w => (w,1))
      })
        .reduceByKey(_ + _).transform(rdd => {
        rdd.sortBy(t => t._2,ascending = false)
      })

      resDStream.print()
      ssc
    }

        /*.map(t =>t.value())
        .flatMap(_.split(" "))
        .map(k => (k,1))
        .reduceByKey(_ + _).transform((rdd) =>{
        rdd.sortBy(t =>t._2,ascending = false)*/

    //创建ssc实例  并做checkpointHA
    //当要使用HA的机制的时候，需要启用checkpoint，让spark自动在某个文件系统上
    //记录程序的相关运行情况，然后每次重启任务的时候，都从相同的位置去读取checkpoint的信息
    //来开启ssc
    val ssc = StreamingContext.getActiveOrCreate(CheckpointPath,creatingFunc)

    /**
      * 当第一次运行代码，处理逻辑的时候，会生成DAG图，
      * DAG图也被保存在checkpoint当中，所以当你第一次
      * 运行的时候发现代码逻辑有问题，手动关闭程序，修改
      * 代码重新运行，往往会报错（因为DAG图被修改）
      * 解决方案：
      *    1/自己管理偏移量
      *    2/在写api的时候，用一些外层的api来封装内部的可能会变化的操作
      *
      */

    ssc.start()
    ssc.awaitTermination()

    }


}
