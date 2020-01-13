package cn.czcxy.study.spark.basic

/**
  * 数组
  */
object ArrayScala {
  def main(args: Array[String]): Unit = {
    println(arrays())
  }

  /**
    * 数组
    */
  def arrays(): StringBuilder = {
    // 定义数组
    var arrays: Array[String] = new Array[String](3)
    arrays(0) = "test0"
    arrays(1) = "test1"
    arrays(2) = "test2"
    for (a <- arrays) {
      println(a)
    }
    // 数组合并
    var two_arrays = Array.concat(arrays, arrays)
    var strVal: StringBuilder = new StringBuilder
    for (index <- 0 to (two_arrays.length - 1)) {
      strVal ++= two_arrays(index)
    }
    return strVal
  }
}
