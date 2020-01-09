package cn.czcxy.study.spark.oo

/**
  * 构造器，不加修饰的变量外界无法访问；只能赋值给内部变量;默认使用val修饰
  *
  * @param id
  * @param name
  * @param age
  */
class StructStudy(val id: Int, val name: String, age: Int) {
  val newAge: Int = age
}

object StructStudy {
  def main(args: Array[String]): Unit = {
    val s = new StructStudy(1, "test", 26)
    println(s.id)
    println(s.name)
    println(s.newAge)
    // println(s.age)
  }
}
