package spark_stream

import Utils.{PropertiesUtil, SparkUtil}
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import spark_stream.SparkKafkaDemo.topics

object UseDirectStream extends App{
  //val sc = SparkUtil.createSparkContext(true,"UseReceiverKafkaStreaming")
  /**
    * 当使用接收器来接受数据的时候，在一段时间内，生成一个block块
    * 这个值，如果过小，会导致有较多的小文件
    * 如果过大，会导致，数据丢失
    * spark.streaming.blockInterval 200ms
    */
//  val ssc = new StreamingContext(sc,Seconds(3))


  //获取数据源
  /**
    *  ssc: StreamingContext,
      zkQuorum: String,
      groupId: String,
      topics: Map[String, Int],
    */


  /*
    API1：
    val kafkaDStream = KafkaUtils
      .createStream(ssc,zkQuorum,groupId,topics,StorageLevel.MEMORY_AND_DISK_SER_2)
      .flatMap(line => line._2.split(" "))
      .map(word => (word,1))
      .reduceByKey(_ + _)
  */


  /**
    *   def createStream[K: ClassTag, V: ClassTag, U <: Decoder[_]: ClassTag, T <: Decoder[_]: ClassTag](
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      topics: Map[String, Int],
      storageLevel: StorageLevel
    ): ReceiverInputDStream[(K, V)] = {
    val walEnabled = WriteAheadLogUtils.enableReceiverLog(ssc.conf)
    new KafkaInputDStream[K, V, U, T](ssc, kafkaParams, topics, walEnabled, storageLevel)
  }
    */

  val Array(brokers, topics) = /*args*/ Array(
    PropertiesUtil.getPropString("kafka.bootstrap.servers"),
    PropertiesUtil.getPropString("kafka.topic.source")
  )

  val sparkConf = new SparkConf().setMaster("local[3]")
    .setAppName("spark-kafka-demo1")
  val ssc = new StreamingContext(sparkConf, Seconds(2))

  /* 第二种方式 */
  /*val spark = SparkSession.builder()
      .appName("spark-kafka-demo1")
      .master("local[2]")
      .getOrCreate()
  // 引入隐式转换方法，允许ScalaObject隐式转换为DataFrame
  import spark.implicits._
  val ssc = new StreamingContext(spark.sparkContext,Seconds(1))*/

  // 设置检查点
  ssc.checkpoint("spark_demo_cp1")

  // Create direct Kafka Stream with Brokers and Topics
  // 注意：这个Topic最好是Array形式的，set形式的匹配不上
  //var topicSet = topics.split(",")/*.toSet*/
  val topicsArr: Array[String] = topics.split(",")

  // set Kafka Properties
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> brokers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> PropertiesUtil.getPropString("kafka.group.id"),
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  /**
    * createStream是Spark和Kafka集成包0.8版本中的方法，它是将offset交给ZK来维护的
    *
    * 在0.10的集成包中使用的是createDirectStream，它是自己来维护offset，
    * 速度上要比交给ZK维护要快很多，但是无法进行offset的监控。
    * 这个方法只有3个参数，使用起来最为方便，但是每次启动的时候默认从Latest offset开始读取，
    * 或者设置参数auto.offset.reset="smallest"后将会从Earliest offset开始读取。
    *
    * 官方文档@see <a href="http://spark.apache.org/docs/2.1.2/streaming-kafka-0-10-integration.html">Spark Streaming + Kafka Integration Guide (Kafka broker version 0.10.0 or higher)</a>
    *
    */


  val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topicsArr, kafkaParams)
  )

//  val outstream: DStream[(String, String)] = stream.map(record => (record.key, record.value))
  val outstream: DStream[(String, Int)] = stream.map(record => (record.key, record.value))
    .flatMap(k => k._2.split(" "))
    .map(k => (k,1))
    .reduceByKey(_ + _)

  outstream.print()

  ssc.start()
  ssc.awaitTermination()
}

