package cn.czcxy.study.spark01

import java.util.{Properties, UUID}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.lang.math.RandomUtils
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}


/**
  * 订单数据生成
  * 创建topic
  * /usr/local/kafka_2.11-2.4.0/bin/kafka-topics.sh --create --zookeeper 123.207.55.47:2181 --replication-factor 1 --partitions 1 --topic saleOrder
  * 创建消费者
  * /usr/local/kafka_2.11-2.4.0/bin/kafka-console-consumer.sh --bootstrap-server 47.93.187.183:9092 --topic saleOrder
  */
object OrderJsonProducer {
  val kafkaProducer: KafkaProducer[String, String] = createKafkaProducer
  val topic: String = "saleOrder"
  val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)


  def main(args: Array[String]): Unit = {
    // TODO 注册JsonMapper
    val forCount: Int = RandomUtils.nextInt(8)
    for (index <- 0 to 8) {
      // TODO 生成销售订单数据
      val saleOrder: SaleOrder = {
        // 订单id 使用随机数生成
        val orderId: String = UUID.randomUUID().toString
        // 省份id
        val provinceId: Int = RandomUtils.nextInt(5) + 1
        // 订单价格
        val orderPrice: Float = RandomUtils.nextInt(34) + 0.5f
        SaleOrder(orderId, provinceId, orderPrice)
      }
      // TODO 将销售订单实体转换为json
      val orderJson: String = mapper.writeValueAsString(saleOrder)
      // 将json转换为实体
      val order: SaleOrder = mapper.readValue(orderJson, classOf[SaleOrder])
      println(orderJson)
      // println(order)
      sendKafka(saleOrder)
    }
  }

  /**
    * 讲销售订单数据发送到kafka
    *
    * @param saleOrder
    */
  def sendKafka(saleOrder: SaleOrder): Unit = {
    val producerRecord: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, saleOrder.orderId, mapper.writeValueAsString(saleOrder))
    kafkaProducer.send(producerRecord)
    // 每次发送数据以后，稍微休息
    Thread.sleep(RandomUtils.nextInt(10) * 100)
  }

  /**
    * 创建kafka生产端
    *
    * @return
    */
  def createKafkaProducer: KafkaProducer[String, String] = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "47.93.187.183:9092")
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val kafkaProducer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
    kafkaProducer
  }
}
