package cn.czcxy.study.spark

object MethodScala {
  // 方法调用
  def main(args: Array[String]): Unit = {
    println(count(1, 2))
  }

  /**
    * 计算两数相加
    *
    * @param a
    * @param b
    * @return
    *
    *
    */
  def count(a: Int, b: Int): Int = {
    val countResult: Int = a + b
    return countResult
  }
}
