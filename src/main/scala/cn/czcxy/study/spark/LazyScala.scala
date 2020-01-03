package cn.czcxy.study.spark

class LazyScala {

}

object LazyScala {
  def init(): Unit = {
    println("call init")
  }

  /**
    * 由lazy修饰的变量在调用的时候才执行
    * 由lazy修饰的变量为惰性变量，是不可变的
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    // val pro = init()
    lazy val pro = init()
    println("after init")
    println(pro)
  }
}
