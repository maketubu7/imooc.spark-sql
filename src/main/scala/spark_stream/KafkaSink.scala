package spark_stream

import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

/**
  * 手动实现一个KafkaSink类，并实例化producer 将数据发送到kafka的对应topic
  * This is the key idea that allows us to work around running into NotSerializableExceptions.
  * Created by make on 2018-08-08 18:50
  */
class KafkaSink[K,V](createProducer: () => KafkaProducer[K, V]) extends Serializable {
  //创建一个 生产者
  lazy val producer = createProducer()

  /** 发送消息 */
  //本质是调用producer.send进行数据发送
  def send(topic : String, key : K, value : V) : Future[RecordMetadata] =
    producer.send(new ProducerRecord[K,V](topic,key,value))
  def send(topic : String, value : V) : Future[RecordMetadata] =
    producer.send(new ProducerRecord[K,V](topic,value))
}
//使用了伴生对象，简单实例化kafkasink
object KafkaSink {
  import scala.collection.JavaConversions._
  def apply[K, V](config: Map[String, Object]): KafkaSink[K, V] = {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[K, V](config)
      sys.addShutdownHook {
        // Ensure that, on executor JVM shutdown, the Kafka producer sends
        // any buffered messages to Kafka before shutting down.
        producer.close()
      }
      producer
    }
    //返回一个producer
    new KafkaSink(createProducerFunc)
  }
  def apply[K, V](config: java.util.Properties): KafkaSink[K, V] = apply(config.toMap)
}


