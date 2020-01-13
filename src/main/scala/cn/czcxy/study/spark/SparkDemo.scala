package cn.czcxy.study.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkDemo {
  def main(args: Array[String]): Unit = {
    val path = "hdfs://aliyun:9000/input/info"
    // 配置信息
    val conf: SparkConf = new SparkConf().setAppName("SparkWC").setMaster("local[*]")
    // 获取上下文信息
    val context: SparkContext = new SparkContext(conf)
    // 获取文件信息
    // val lineInfo: RDD[String] = context.textFile(args(0))
    val lineInfo: RDD[String] = context.textFile(path)
    // 获取切分后的单词
    val words: RDD[String] = lineInfo.flatMap(_.split(" "))
    // 组成元组
    val paired: RDD[(String, Int)] = words.map((_, 1))
    // 按照key聚合
    val reduce: RDD[(String, Int)] = paired.reduceByKey(_ + _)
    // 排序
    val res: RDD[(String, Int)] = reduce.sortBy(_._2, false)
    println(res.collect().toBuffer)
    // 保存文件
    // res.saveAsTextFile(args(1))
    context.stop()
  }
}
