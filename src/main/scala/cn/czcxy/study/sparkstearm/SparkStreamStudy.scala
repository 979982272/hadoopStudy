package cn.czcxy.study.sparkstearm

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * sparkstreaming 学习，消费kafka消息
  */
object SparkStreamStudy {
  val CHECK_POING_PATH = "E://spark/sparkstreaming/checkpoint_sparkstreamingstudy/"

  /**
    * 处理来自kafka的消息
    *
    * @param ssc
    */
  def processData(ssc: StreamingContext): Unit = {
    /**
      * TODO 2. 创建kafka链接
      */
    // kafka配置信息
  //  val kafkaParms: Map[String, Object] = Map("metadata.broker.list" -> " 47.93.187.183:9092")

    val kafkaParms = Map[String, Object](
      "bootstrap.servers" -> "47.93.187.183:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "g1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // kafka中对应的topic
    val topic: Set[String] = Set("order-topic")
    // 通过createDirectStream构建
    //    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParms, topic)
    //    kafkaDStream.transform((rdd, time) => {
    //      print(1)
    //      null
    //    })

    val map = scala.collection.mutable.Map[TopicPartition, String]()
    map.put(new TopicPartition("order-topic", 0), "s102")
    val locStra: LocationStrategy = LocationStrategies.PreferFixed(map)

    val topicPartitions = scala.collection.mutable.ArrayBuffer[TopicPartition]()
    topicPartitions.+=(new TopicPartition("order-topic", 0))
    val value: ConsumerStrategy[String, String] = ConsumerStrategies.Assign[String, String](topicPartitions, kafkaParms)
    //创建kakfa直向流
    val kafkaDStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      locStra,
      value
    )
    kafkaDStream.transform((rdd, time) => {
      rdd
        .mapPartitions(iter => {
          // 针对每个分区数据操作： TODO：RDD中每个分区数据对应于Kafka Topic中每个分区的数据
          iter.map(item => {
            val array = item.value().split(",")
            // 返回二元组，按照省份ID进行统计订单销售额，所以省份ID为Key
            (array(0), array(1))
          })
        })
    }).print()

  }

  def main(args: Array[String]): Unit = {
    // TODO 1. 创建StreamingContext对象
    var cxt: StreamingContext = StreamingContext.getOrCreate(CHECK_POING_PATH, () => {
      // 创建spark基础配置信息
      val conf: SparkConf = new SparkConf()
        .setMaster("local[3]") // 启动三个线程Thread运行应用
        .setAppName("MapWithStateKafkaStreaming")
      val ssc = new StreamingContext(conf, Seconds(5))
      // 设置日志级别
      ssc.sparkContext.setLogLevel("WARN")
      // 设置检查点信息
      //  ssc.checkpoint(CHECK_POING_PATH)
      // 处理kafka信息
      processData(ssc)
      ssc
    })
    // TODO 2. 启动应用
    cxt.start()
    cxt.awaitTermination()
    cxt.stop(stopSparkContext = true, stopGracefully = true)
  }
}
