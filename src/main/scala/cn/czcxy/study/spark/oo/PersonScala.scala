package cn.czcxy.study.spark.oo

class PersonScala {
  // id 只读
  // 由val修饰的变量，无法修改所以必须复制初始化
  val id: Int = 0

  // name 可读可写
  var name: String = _

  // 私有属性，本对象与伴生对象可访问
  private var sex: String = _

  // 对象私有属性，只对本对象可访问
  private[this] var age: Int = _
}

/**
  * 伴生对象
  */
object PersonScala {
  def main(args: Array[String]): Unit = {
    var p = new PersonScala
    p.name = ""
  }
}

/**
  * 非伴生对象
  */
object test {
  def main(args: Array[String]): Unit = {
    // 无法访问 private修饰的属性
    var p = new PersonScala

  }
}
