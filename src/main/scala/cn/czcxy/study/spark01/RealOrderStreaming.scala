package cn.czcxy.study.spark01

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import cn.czcxy.study.utils.{KafkaConstantUtil, ObjectMapperUtil, RedisUtil}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import redis.clients.jedis.Jedis

/**
  * 实时消费订单信息
  */
object RealOrderStreaming {

  // 检查点
  val CHECK_POINT_PATH: String = "hdfs://aliyun:9000//spark/sparkstreaming/checkpoint/RealOrderStreaming/0009"

  // 设置SparkStreaming应用的Batch Interval
  val STREAMING_BATCH_INTERVAL = Seconds(5)

  // 设置窗口时间间隔
  val STREAMING_WINDOW_INTERVAL = STREAMING_BATCH_INTERVAL * 30

  // 设置滑动时间间隔
  val STREAMING_SLIDER_INTERVAL = STREAMING_BATCH_INTERVAL * 2

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
        .setMaster("local[3]")
        .setAppName("RealOrderStreaming")
        // 每秒钟获取Topic中每个分区的最大条目, 比如设置1000条，Topic三个分区，每秒钟最大数据量为30000, 批处理时间间隔为5秒，则每批次处理数据量最多 150000
        .set("spark.streaming.kafka.maxRatePerPartition", "10000")
        // 设置数据处理本地性等待超时时间
        .set("spark.locality.wait", "100ms")
        // 设置使用Kryo序列化, 针对非基本数据类型，进行注册
        //  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        //  .registerKryoClasses(Array(classOf[SaleOrder]))
        // 设置启用反压机制
        .set("spark.streaming.backpressure.enabled", "true")
        // 设置Driver JVM的GC策略
        .set("spark.driver.extraJavaOptions", "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC")
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
    // 连接数据库的信息
    val (url, props) = {
      val jdbcUrl = "jdbc:mysql://123.207.55.47:3306"
      val properties: Properties = new Properties()
      properties.put("user", "base")
      properties.put("password", "tudou123-")
      (jdbcUrl, properties)
    }
    // TODO 1 进行转换操作
    val orderDStream: DStream[(Int, SaleOrder)] = kafkaDStream.transform(rdd => {
      rdd.mapPartitions(iter => {
        iter.map(item => {
          val mapper: ObjectMapper = ObjectMapperUtil.getInstance()
          val order: SaleOrder = mapper.readValue(item.value(), classOf[SaleOrder])
          (order.provinceId, order)
        })
      })
    })
    // TODO 2.1 通过updateStateByKey函数进行统计累加
    val orderDStreamByKey: DStream[(Int, Float)] = orderDStream.updateStateByKey((time: Time, provinceId: Int, currentSaleOrder: Seq[SaleOrder], oldSaleOrder: Option[Float]) => {
      val currentPrice: Float = currentSaleOrder.map(_.orderPrice).sum
      val oldPrice: Float = oldSaleOrder.getOrElse(0.0f)
      Some(currentPrice + oldPrice)
    }, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    // TODO 2.2 降低分区数据，讲结果存储到redis中
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
        /* val sparkSession: SparkSession = SparkSession.builder()
           .config(rdd.sparkContext.getConf)
           .config("spark.sql.shuffle.partitions", "6")
           .getOrCreate()
         import sparkSession.implicits._
         val totalPrices: DataFrame = rdd.map((_._1)).toDF()
         val top3Prices: Dataset[Row] = totalPrices.orderBy($"value".desc).limit(3)
         top3Prices.write.mode(SaveMode.Overwrite).jdbc(url, "spark.order_top3_price", props)
      }
    })*/
        // TODO 3.实时统计出最近时间段内订单量最高的前十个省份

        // 窗口批次操作
        orderDStream.window(STREAMING_BATCH_INTERVAL, STREAMING_SLIDER_INTERVAL)
          .foreachRDD((rdd, time) => {
            println("-------------------------------------------")
            println(s"windows Batch Time: ${new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS").format(new Date(time.milliseconds))}")
            if (!rdd.isEmpty()) {
              println("-------*****************************")
              println(rdd.collect().toBuffer)
              // 数据缓存
              rdd.cache()
              // 创建sqlsession
              val sparkSession: SparkSession = SparkSession.builder()
                .config(rdd.sparkContext.getConf)
                .config("spark.sql.shuffle.partitions", "6")
                .getOrCreate()
              // 引入SparkSession中的隐式转换
              import sparkSession.implicits._

              val saleOrder: DataFrame = rdd.map(_._2).toDF()
              println(saleOrder.collect().toBuffer)
              // 通过省份编码分组，排序 取前十个
              val frame: DataFrame = saleOrder.groupBy($"provinceId").sum("orderPrice")
              println(frame.collect().toBuffer)
              println(frame.orderBy($"sum(orderPrice)".desc).collect().toBuffer)
              val top10ProvinceOrderCountDF: Dataset[Row] = saleOrder.groupBy($"provinceId").sum("orderPrice").orderBy($"sum(orderPrice)".desc).limit(3)
              // val top10ProvinceOrderCountDF: Dataset[Row] = saleOrder.groupBy($"provinceId").count().orderBy($"count".desc).limit(3)
              // 写入到数据库
              top10ProvinceOrderCountDF.write.mode(SaveMode.Overwrite).jdbc(url, "spark.order_top10_count", props)
              // 释放rdd数据
              rdd.unpersist()
            }
          })
      }

      def main(args: Array[String]): Unit = {
        sparkOperation(args)(processKafkaData)
      }

    }
