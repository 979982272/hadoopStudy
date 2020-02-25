package cn.czcxy.study.spark01

import cn.czcxy.study.spark01.OrderJsonProducer.mapper
import cn.czcxy.study.utils.{KafkaConstantUtil, ObjectMapperUtil}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 实时消费订单信息
  */
object RealOrderStreaming {

  // 检查点
  val CHECK_POINT_PATH: String = "hdfs://aliyun:9000//spark/sparkstreaming/checkpoint_RealOrderStreaming/"

  // 设置SparkStreaming应用的Batch Interval
  val STREAMING_BATCH_INTERVAL = Seconds(5)

  // topic
  val topic: String = "saleOrder"

  /**
    * 贷出模式 实际处理资源的开启和关闭
    *
    * @param args
    * @param operation 用户函数，处理数据
    */
  def sparkOperation(args: Array[String])(operation: StreamingContext => Unit): Unit = {
    // 创建StreamingContext实例对象
    val creatingFunc = () => {
      val sparkConf: SparkConf = new SparkConf()
      sparkConf.setMaster("local[3]")
      sparkConf.setAppName("RealOrderStreaming")
      val ssc: StreamingContext = new StreamingContext(sparkConf, STREAMING_BATCH_INTERVAL)
      // 设置检查点
      ssc.checkpoint(CHECK_POINT_PATH)
      // 设置日志级别
      ssc.sparkContext.setLogLevel("WARN")
      // 调用用户函数
      operation(ssc)
      ssc
    }
    // 创建hadoop配置
    val hadoopConf: Configuration = {
      var conf = SparkHadoopUtil.get.conf
      conf.set("dfs.client.use.datanode.hostname", "true")
      conf
    }
    var context: StreamingContext = null
    try {
      context = StreamingContext.getActiveOrCreate(CHECK_POINT_PATH, creatingFunc, hadoopConf)
      // 启动应用
      context.start()
      // 等待应用终止
      context.awaitTermination()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // 关闭资源
      if (null != context) context.stop(true, true)
    }

  }

  /**
    * 贷出模式中的用户函数
    *
    * @param ssc
    */
  def processKafkaData(ssc: StreamingContext): Unit = {
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaConstantUtil.createKafkaDStream(ssc, topic)
    val orderDStream: DStream[(Int, SaleOrder)] = kafkaDStream.transform(rdd => {
      rdd.mapPartitions(iter => {
        iter.map(item => {
          val mapper: ObjectMapper = ObjectMapperUtil.getInstance()
          val order: SaleOrder = mapper.readValue(item.value(), classOf[SaleOrder])
          (order.provinceId, order)
        })
      })
    })
    orderDStream.print()
  }

  def main(args: Array[String]): Unit = {
    sparkOperation(args)(processKafkaData)
  }

}
