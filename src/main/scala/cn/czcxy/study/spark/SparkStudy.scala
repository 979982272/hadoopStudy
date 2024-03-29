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
    // 调用flatten把多个list合并为一个list
    val flattenRes = rdd3.collect().flatten.flatMap(_.split(" "))
    println(flattenRes.toBuffer)


    val rdd4: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))
    val rdd5: RDD[Int] = sc.parallelize(List(4, 5, 6, 7, 8))
    // 求并集
    var res: RDD[Int] = rdd4 union rdd5
    println(res.collect().toBuffer)
    // 去重
    println(res.distinct().collect().toBuffer)
    // 求交集
    res = rdd4 intersection rdd5
    println(res.collect().toBuffer)

    val rdd6: RDD[(String, Int)] = sc.parallelize(List(("test1", 1), ("test2", 2), ("test3", 3)))
    val rdd7: RDD[(String, Int)] = sc.parallelize(List(("test4", 4), ("test5", 2), ("test3", 6)))
    // 求join 查询出相同的
    println((rdd6 join rdd7).collect().toBuffer)
    // 左连接 查询出左边的
    println((rdd6 leftOuterJoin rdd7).collect().toBuffer)
    // 右连接 查询出右边的
    println((rdd6 rightOuterJoin rdd7).collect().toBuffer)
    // 先求并集而后按照key分组
    println((rdd6 union rdd7).groupByKey().collect().toBuffer)
    println((rdd6 union rdd7).groupByKey().mapValues(_.sum).collect().toBuffer)
    println((rdd6 union rdd7).reduceByKey(_ + _).collect().toBuffer)
    sc.stop()
  }
}
