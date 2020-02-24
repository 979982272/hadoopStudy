package cn.czcxy.study.sparkstearm

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *sparkstreaming 简单程序，链接 47.93.187.183 7777 的socket端口，在47.93.187.183 安装nc
  * yum install nc
  *  nc -lk 7777 启动应用 ，在控制台 输入 这边就能收到 -lk(小写LK)   aaa
  */
object SparkStreamStudy01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[3]") // 启动三个线程Thread运行应用
      .setAppName("SparkStreamStudy")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("47.93.187.183", 7777)
    val errorLines: DStream[String] = lines.filter(_.contains("error"))
    errorLines.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
