package cn.czcxy.study.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkDemo {
  def main(args: Array[String]): Unit = {
    // 单机运行
    //./spark-submit --master "spark://aliyun:7077"  --class cn.czcxy.study.spark.SparkDemo /usr/local/hadoop-study-0.0.1-SNAPSHOT.jar hdfs://aliyun:9000/input/it.log.2019-11-05 hdfs://aliyun:9000/output/test145
    // 集群运行
    //./spark-submit --master "spark://aliyun:7077" --deploy-mode cluster  --class cn.czcxy.study.spark.SparkDemo /usr/local/hadoop-study-0.0.1-SNAPSHOT.jar hdfs://aliyun:9000/input/info hdfs://aliyun:9000/output/test145
    // val inputPath = "hdfs://aliyun:9000/input/info"
    // val outputPath = "hdfs://aliyun:9000/output/test14s"
    // local[2] 代表需要用多少个woker去运行
    val inputPath = args(0)
    val outputPath = args(1)
    // 配置信息
    val conf: SparkConf = new SparkConf().setAppName("SparkWC").setMaster("local[2]")
    // 获取上下文信息
    val context: SparkContext = new SparkContext(conf)
    // 获取文件信息
    // val lineInfo: RDD[String] = context.textFile(args(0))
    val lineInfo: RDD[String] = context.textFile(inputPath)
    // 获取切分后的单词
    val words: RDD[String] = lineInfo.flatMap(_.split(" "))
    // 组成元组
    val paired: RDD[(String, Int)] = words.map((_, 1))
    // 按照key聚合
    val reduce: RDD[(String, Int)] = paired.reduceByKey(_ + _)
    // 排序
    val res: RDD[(String, Int)] = reduce.sortBy(_._2, false)
    // println(res.collect().toBuffer)
    // 保存文件
    res.saveAsTextFile(outputPath)
    context.stop()
  }
}
