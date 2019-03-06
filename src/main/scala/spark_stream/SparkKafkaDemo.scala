package spark_stream

import java.util.Properties

import Utils.PropertiesUtil
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.LocationStrategies._


object SparkKafkaDemo extends App {
  // default a Logger Object
  val LOG = org.slf4j.LoggerFactory.getLogger(SparkKafkaDemo.getClass)

  /*if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
      """.stripMargin)
      System.exit(1)
  }*/
  // 设置日志级别
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)

  val Array(brokers, topics, outTopic) = /*args*/ Array(
    PropertiesUtil.getPropString("kafka.bootstrap.servers"),
    PropertiesUtil.getPropString("kafka.topic.source"),
    PropertiesUtil.getPropString("kafka.topic.sink")
  )

  // Create context
  /* 第一种方式 */
  val sparkConf = new SparkConf().setMaster("local[2]")
    .setAppName("spark-kafka-demo1")
  val ssc = new StreamingContext(sparkConf, Milliseconds(1000))

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
    * 在0.10的集成包中使用的是createDirectStream，它是自己来维护offset，在这个版本中
    * zkCli中是看不到每个分区，到底消费到了那个偏移量，而在老的版本中，是可以看到的
    * 速度上要比交给ZK维护要快很多，但是无法进行offset的监控。
    * 这个方法只有3个参数，使用起来最为方便，但是每次启动的时候默认从Latest offset开始读取，
    * 或者设置参数auto.offset.reset="smallest"后将会从Earliest offset开始读取。
    *
    * 官方文档@see <a href="http://spark.apache.org/docs/2.1.2/streaming-kafka-0-10-integration.html">Spark Streaming + Kafka Integration Guide (Kafka broker version 0.10.0 or higher)</a>
    *
    */

  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topicsArr, kafkaParams)
  )

  /** Kafak sink */
  //set producer config
  val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
    val kafkaProducerConfig = {
      val p = new Properties()
      p.setProperty("bootstrap.servers", brokers)
      p.setProperty("key.serializer", classOf[StringSerializer].getName)
      p.setProperty("value.serializer", classOf[StringSerializer].getName)
      p
    }
    LOG.info("kafka producer init done!")
    // 广播KafkaSink 传入kafkaProducerConfig，在kafkaSink中实例化producer
    ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
  }

  var jsonObject = new JSONObject()
  //对传入的流中的数据，进行筛选和逻辑处理
  stream.filter(record => {
    // 过滤掉不符合要求的数据
    try {
      jsonObject = JSON.parseObject(record.value)
    } catch {
      case e: Exception => {
        LOG.error("转换为JSON时发生了异常！\t{}", e.getMessage)
      }
    }
    // 如果不为空字符时，为null，返回false过滤，否则为true通过
    StringUtils.isNotEmpty(record.value) && null != jsonObject
  }).map(record => {
    //这个地方可以写自己的业务逻辑代码，因为本次是测试，简单返回一个元组
    jsonObject = JSON.parseObject(record.value)
    // 返出一个元组，(时间戳，json的数据日期，json的关系人姓名)
    (System.currentTimeMillis(),
      jsonObject.getString("date_dt"),
      jsonObject.getString("relater_name")
    )
  }).foreachRDD(rdd => {
    if (!rdd.isEmpty()) {
      rdd.foreach(kafkaTuple => {
        //向Kafka发送数据，outTopic，value，也就是我们kafkasink的第二种send方法
        //取出广播的value 调用send方法 对每个数据进行发送 和 打印
        kafkaProducer.value.send(
          outTopic,
          kafkaTuple._1 + "\t" + kafkaTuple._2 + "\t" + kafkaTuple._3
        )
        //同时将信息打印到控制台，以便查看
        println(kafkaTuple._1 + "\t" + kafkaTuple._2 + "\t" + kafkaTuple._3)
      })
    }
  })

  // 启动streamContext
  ssc.start()
  //一直等待数据  直到关闭
  ssc.awaitTermination()

}
