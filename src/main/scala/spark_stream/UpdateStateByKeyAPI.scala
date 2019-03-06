package spark_stream

import Utils.{PropertiesUtil, PropertiesUtil_ka_str, SparkUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.HashPartitioner
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object UpdateStateByKeyAPI {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtil.createSparkContext(true,"DStream2RddAPI")

    val CheckpointPath = "datas/checkpoint/DStream3"

    def creatingFunc() = {

      val ssc = new StreamingContext(sc,Seconds(2))

      ssc.checkpoint(CheckpointPath)

      val  topics: String = "producer_consumer_demo1"
      val brokers = PropertiesUtil_ka_str.getPropString("kafka.bootstrap.servers")

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

      //val partition: HashPartitioner = new HashPartitioner(3)
      val addfunc: (Seq[Int], Option[Long]) => Some[Long] = (values:Seq[Int], state:Option[Long]) =>{
        val sum = values.sum
        val presum = state.getOrElse(0L)
        val newsum = sum + presum
        Some(newsum)
      }

      val resDStream: DStream[(String, Long)] = kafka_streamDStream
        .mapPartitions(iter =>{
        iter.map(k => k.value())
          .flatMap(_.split(" "))
          .map(w => (w,1))
      }).reduceByKey(_ + _)
        .updateStateByKey(addfunc)

      //resDStream.print()
      resDStream.foreachRDD((rdd,time) =>{
        println(s"----------------当前时间为：${time}----------------")
        //比如说:某些key不打印，某些值过于小也可以不打印，或者打印排序后的前5
        rdd.filter(t =>{
          t._2 > 6
        }).foreach(println)
      })
      ssc
    }

    val ssc = StreamingContext.getActiveOrCreate(CheckpointPath,creatingFunc)

    ssc.start()
    ssc.awaitTermination()

  }


}
