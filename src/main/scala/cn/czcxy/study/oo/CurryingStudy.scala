package cn.czcxy.study.oo

import ContextPredefStudy._

/**
  * 柯里化
  * 相当于java中的重载，默认值
  */
class CurryingStudy {

}

// 需要定义在调用程序之前 scala由上往下执行
//object Context {
//  implicit val a: Int = 6
//}

object CurryingStudy {
  def main(args: Array[String]): Unit = {
    import ContextPredefStudy.a
    println(add(3, 5))
    println(add1(3)(5))

    // 由于上边context中声明了 隐式变量a，然后再17行映入 所以程序会自动匹配顶替调add3中y的默认值
    println(add3(3))
    println(add3(3)(5))

    var x = add2(3)
    //  println(x("3"))

    println(add3("3"))
  }

  def add(x: Int, y: Int) = x + y

  def add1(x: Int)(y: Int) = x + y

  def add2(x: Int) = (y: Int) => x + y

  // 声明y默认值为5 可以不输入 直接add3(3) 调用
  def add3(x: Int)(implicit y: Int = 5) = x + y
}

