package cn.czcxy.study.spark.basic

/**
  * def定义的为方法，val定义的为函数
  */
object MethodScala {
  // 方法调用
  def main(args: Array[String]): Unit = {
    var contro = new ControllerClassScala
    var result = contro.controller(count)
    println(result)
    result = contro.controller(reduce)
    println(result)
    result = contro.controller(multiply)
    println(result)
    result = contro.controller(divide)
    println(result)
  }

  // 闭包通常来讲可以简单的认为是可以访问一个函数里面局部变量的另外一个函数。
  val multiply = (a: Int, b: Int) => a * b

  // 除法
  var divide = (a: Int, b: Int) => {
    // 在闭包函数中，以最后一行的值作为返回结果
    print("调用除法结果:")
    a / b
  }

  // 加法
  var count = (a: Int, b: Int) => a + b

  // 减法
  var reduce = (a: Int, b: Int) => a - b

  //  // 方法控制器;控制传入函数类型
  //  def controller(met: (Int, Int) => Int): Int = {
  //    return met(10, 5)
  //  }
}
