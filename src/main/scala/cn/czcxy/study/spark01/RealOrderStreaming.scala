package cn.czcxy.study.spark01

import java.text.SimpleDateFormat
import java.util.Date

import cn.czcxy.study.utils.{KafkaConstantUtil, ObjectMapperUtil, RedisUtil}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import redis.clients.jedis.Jedis

/**
  * 实时消费订单信息
  */
object RealOrderStreaming {

  // 检查点
  val CHECK_POINT_PATH: String = "hdfs://aliyun:9000//spark/sparkstreaming/checkpoint/RealOrderStreaming/0004"

  // 设置SparkStreaming应用的Batch Interval
  val STREAMING_BATCH_INTERVAL = Seconds(5)

  // topic
  val topic: String = "saleOrder"

  // redis客户端
  val jedis: Jedis = RedisUtil.getInstance().getResource

  // 存储Redis中实时统计销售额
  val REDIS_KEY_ORDERS_TOTAL_PRICE = "orders:total:price"

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
    // TODO 获取kafka链接
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaConstantUtil.createKafkaDStream(ssc, topic)
    // TODO 进行转换操作
    val orderDStream: DStream[(Int, SaleOrder)] = kafkaDStream.transform(rdd => {
      rdd.mapPartitions(iter => {
        iter.map(item => {
          val mapper: ObjectMapper = ObjectMapperUtil.getInstance()
          val order: SaleOrder = mapper.readValue(item.value(), classOf[SaleOrder])
          (order.provinceId, order)
        })
      })
    })
    // TODO 通过updateStateByKey函数进行统计累加
    val orderDStreamByKey: DStream[(Int, Float)] = orderDStream.updateStateByKey((time: Time, provinceId: Int, currentSaleOrder: Seq[SaleOrder], oldSaleOrder: Option[Float]) => {
      val currentPrice: Float = currentSaleOrder.map(_.orderPrice).sum
      val oldPrice: Float = oldSaleOrder.getOrElse(0.0f)
      Some(currentPrice + oldPrice)
    }, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    // TODO 降低分区数据，讲结果存储到redis中
    orderDStreamByKey.foreachRDD((rdd, Time) => {
      println("-------------------------------------------")
      println(s"Batch Time: ${new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS").format(new Date(Time.milliseconds))}")
      if (!rdd.isEmpty()) {
        rdd.coalesce(1).foreachPartition(item => {
          item.foreach {
            // item 是一个元祖里边有两个值，使用case分别把_1 插入到provinceId,_2 插入到totalPrice
            // item里边的两个值 ，分别代表key 和统计返回的结果
            case (provinceId, totalPrice) => {
              jedis.hset(REDIS_KEY_ORDERS_TOTAL_PRICE, provinceId.toString, totalPrice.toString)
              println(s"provinceId=$provinceId,totalPrice=$totalPrice")
            }
          }
        })
      }
    })
  }

  def main(args: Array[String]): Unit = {
    sparkOperation(args)(processKafkaData)
  }

}
