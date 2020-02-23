package cn.czcxy.study.sparkstearm

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.collection.immutable.StringOps

/**
  * sparkstreaming 学习，消费kafka消息
  */
object SparkStreamStudy {
  // 检查点路径
  val CHECK_POING_PATH = "hdfs://aliyun:9000//spark/sparkstreaming/checkpoint_sparkstreamingstudy/"

  /**
    * 创建kafka连接
    *
    * @param ssc
    * @return
    */
  def createKafkaDStream(ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    // kafka配置信息
    val kafkaParms = Map[String, Object](
      "bootstrap.servers" -> "47.93.187.183:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "g1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val map = scala.collection.mutable.Map[TopicPartition, String]()
    map.put(new TopicPartition("order-topic", 0), "s102")
    val locationStrategy: LocationStrategy = LocationStrategies.PreferFixed(map)
    // 创建topic
    val topicPartitions = scala.collection.mutable.ArrayBuffer[TopicPartition]()
    topicPartitions.+=(new TopicPartition("order-topic", 1))
    val consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Assign[String, String](topicPartitions, kafkaParms)
    //创建kakfa直向流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      locationStrategy,
      consumerStrategy
    )
    kafkaDStream
  }

  /**
    * 处理来自kafka的消息
    *
    */
  def processData(kafkaDStream: InputDStream[ConsumerRecord[String, String]]): Unit = {
    kafkaDStream.transform((rdd, time) => {
      rdd
        .filter(msg => msg.value().split(",").length > 0)
        .mapPartitions(iter => {
          // 针对每个分区数据操作： TODO：RDD中每个分区数据对应于Kafka Topic中每个分区的数据
          iter.map(item => {
            // print(item.value())
            val Array(province, count) = item.value().split(",")
            // 返回二元组，按照省份ID进行统计订单销售额，所以省份ID为Key
            (province, new StringOps(count).toInt)
          })
        })
    }).updateStateByKey((value: Seq[Int], state: Option[String]) => {
      val province: String = state.getOrElse("0.0")
      val count = value.sum
      Some(province + count)
    }).foreachRDD((rdd, time) => {
      println("-------------------------------------------")
      println(s"Batch Time: ${new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS").format(new Date(time.milliseconds))}")
      if (!rdd.isEmpty()) {
        rdd.coalesce(1).foreachPartition(iter => iter.foreach(it=>{print("sss")}))
      }
      println("ddddd")
    })
    /*val unit: DStream[(String, String)] = value.updateStateByKey((value: Seq[Int], state: Option[String]) => {
      val province: String = state.getOrElse("0.0")
      val count = value.sum
      Some(province + count)
    })
    value.foreachRDD((rdd, time) => {
      println("-------------------------------------------")
      println(s"Batch Time: ${new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS").format(new Date(time.milliseconds))}")
      if (!rdd.isEmpty()) {
        rdd.coalesce(1).foreachPartition(iter => iter.foreach(it=>{print("sss")}))
      }
      println("ddddd")
    })*/
  }

  /**
    * 创建StreamingContext
    *
    * @return
    */
  def createStreamingContext(): StreamingContext = {
    // 设置hadoop配置信息
    val hadoopConf: Configuration = SparkHadoopUtil.get.conf
    // 使用域名访问hadoop而不是ip
    hadoopConf.set("dfs.client.use.datanode.hostname", "true")
    // 构建StreamingContext
    var cxt: StreamingContext = StreamingContext.getOrCreate(CHECK_POING_PATH, () => {
      // 创建spark基础配置信息
      val conf: SparkConf = new SparkConf()
        .setMaster("local[3]") // 启动三个线程Thread运行应用
        .setAppName("SparkStreamStudy")
      val ssc = new StreamingContext(conf, Seconds(10))
      // 设置检查点信息
      ssc.checkpoint(CHECK_POING_PATH)
      process(ssc)
      ssc
    }, hadoopConf)
    cxt
  }

  def main(args: Array[String]): Unit = {
    // TODO 1. 创建StreamingContext
    val cxt: StreamingContext = createStreamingContext()
    // TODO 2. 启动Streaming应用
    cxt.start()
    cxt.awaitTermination()
    cxt.stop(stopSparkContext = true, stopGracefully = true)
    /*// TODO 3. 创建kafka链接
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = createKafkaDStream(cxt)
    // TODO 4. 消费kafka消息
    processData(kafkaDStream)*/
  }

  def process(cxt: StreamingContext): Unit ={
    // TODO 3. 创建kafka链接
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = createKafkaDStream(cxt)
    // TODO 4. 消费kafka消息
    processData(kafkaDStream)
  }
}
