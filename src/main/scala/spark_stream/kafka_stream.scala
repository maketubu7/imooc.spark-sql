  package spark_stream

  import Utils.{PropertiesUtil, PropertiesUtil_ka_str, SparkUtil}
  import org.apache.kafka.clients.consumer.ConsumerRecord
  import org.apache.kafka.common.serialization.StringDeserializer
  import org.apache.spark.streaming.dstream.{DStream, InputDStream}
  import org.apache.spark.streaming.kafka010.KafkaUtils
  import org.apache.spark.streaming.{Seconds, StreamingContext}


  import org.apache.spark.streaming.kafka010.ConsumerStrategies._
  import org.apache.spark.streaming.kafka010.LocationStrategies._

  object kafka_stream {
    def main(args: Array[String]): Unit = {
      val sc =SparkUtil.createSparkContext(true,"kafka_stream")

      val ssc = new StreamingContext(sc,Seconds(5))



      //设置相关参数

     val  topics: String = PropertiesUtil_ka_str.getPropString("kafka.topic.source")
     val brokers = PropertiesUtil_ka_str.getPropString("kafka.bootstrap.servers")

      //具体写法参照 http://spark.apache.org/docs/2.2.0/streaming-kafka-0-10-integration.html
      val topicarr = topics.split(",")
      val kafkaParams: Map[String, Object] = Map[String,Object](
        "bootstrap.servers" -> brokers,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> PropertiesUtil.getPropString("kafka.group.id"),
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )

      val kafka_streamDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
        ssc,
        PreferConsistent,
        //Subscribe的构造函数为三个参数，但是可以省略offsets 源码可以看到
        Subscribe[String,String](topicarr,kafkaParams))
      //最后的格式为(（offset,partition,value）,1),这样的数据类型
      // 可以看到每条数据的偏移量和所在的分区
      val resDStream: DStream[((Long, Int, String), Int)] = kafka_streamDStream.map(line =>
        (line.offset(), line.partition(), line.value())).flatMap(t =>{
          t._3.split(" ").map(word => (t._1,t._2,word))
      })
        .map(k => ((k._1,k._2,k._3),1))
        .reduceByKey(_ + _)

      resDStream.print()

      ssc.start()
      ssc.awaitTermination()

    }


  }
