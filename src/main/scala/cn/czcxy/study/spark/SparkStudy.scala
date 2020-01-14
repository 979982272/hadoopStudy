package cn.czcxy.study.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkStudy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkStudy").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 通过并行化生成rdd
    val info: RDD[Int] = sc.parallelize(List(8, 10, 3, 2, 9, 4, 7, 5, 6, 1))
    // 对rdd的每个元素乘2然后排序
    val multiplyRes: RDD[Int] = info.map(_ * 2)
    // 过滤出来大于等于十
    val filterRes: RDD[Int] = multiplyRes.filter(_ >= 10)
    // 排序
    val sortRes: RDD[Int] = filterRes.sortBy(x => x, false)
    println(sortRes.collect().toBuffer)

    // 将rdd2 切分压平
    val rdd2: RDD[String] = sc.parallelize(Array("a b c", "d e f", "h i j"))
    val flatRes = rdd2.flatMap(_.split(" "))
    println(flatRes.collect().toBuffer)

    // 将rdd3 切分压平
    val rdd3: RDD[List[String]] = sc.parallelize(List(List("a b c", "a b b"), List("e f g", "f s d")))
    val flattenRes = rdd3.collect().flatten.flatMap(_.split(" "))
    println(flattenRes.toBuffer)
    sc.stop()
  }
}
