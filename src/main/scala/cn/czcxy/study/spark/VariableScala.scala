package cn.czcxy.study.MethodScala

object VariableScala {
  /**
    * 变量章节
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    // 在scala中var/val可以不指定类型，由赋值推导出变量类型
    // var 类型为变量;初始化值之后可以重新赋值
    var myVar: String = "test01"
    myVar = "test01"
    // val 类型为常量，初始化值之后不可以重新赋值;重新复制在程序编译期间就会报错
    val myVal: String = "test03"
    // myVal = "test04"
    // 元祖
    var po = (10, "test04")
    println(po)
  }
}
