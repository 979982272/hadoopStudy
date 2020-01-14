package cn.czcxy.study.basic

/**
  * 元祖
  */
object TupleScala {
  def main(args: Array[String]): Unit = {
    var tuple = (1, 2, 3)
    println(tuple._1 + tuple._2 + tuple._3)
    // 迭代
    tuple.productIterator.foreach(t => println(t))
  }
}
