package cn.czcxy.study.utils

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._

/**
  * kafka 配置参数
  */
object KafkaConstantUtil {
  // kafka地址
  val BOOTSTRAP_SERVERS: String = "47.93.187.183:9092"

  // OFFSET
  val AUTO_OFFSET_RESET: String = "latest"

  // 组别id
  val GROUP_ID: String = "g1"

  def createKafkaDStream(ssc: StreamingContext, topic: String): InputDStream[ConsumerRecord[String, String]] = {
    // kafka配置信息
    val kafkaParms = Map[String, Object](
      "bootstrap.servers" -> BOOTSTRAP_SERVERS,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
     // "group.id" -> GROUP_ID,
      "auto.offset.reset" -> AUTO_OFFSET_RESET,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val map = scala.collection.mutable.Map[TopicPartition, String]()
    map.put(new TopicPartition(topic, 0), "s102")
    val locationStrategy: LocationStrategy = LocationStrategies.PreferFixed(map)
    // 创建topic
    val topicPartitions = scala.collection.mutable.ArrayBuffer[TopicPartition]()
    topicPartitions.+=(new TopicPartition(topic, 0))
    val consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Assign[String, String](topicPartitions, kafkaParms)
    //创建kakfa直向流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      locationStrategy,
      consumerStrategy
    )
    kafkaDStream
  }
}
