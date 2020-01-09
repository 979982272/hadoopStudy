package cn.czcxy.study.spark.oo

class ApplyStudy(val name: String, val sex: Int, val age: Int) {

}

object ApplyStudy {
  // 注入用在match 模式匹配
  def apply(name: String, sex: Int, age: Int): ApplyStudy =
    new ApplyStudy(name, sex, age)

  // 反注入
  def unapply(applyStudy: ApplyStudy): Option[(String, Int, Int)] = {
    if (applyStudy == None) {
      None
    } else {
      Some(applyStudy.name, applyStudy.sex, applyStudy.age)
    }
  }
}

object Test {
  def main(args: Array[String]): Unit = {
    // 调用到apply
    val apply = ApplyStudy("test", 1, 2)
    apply match {
      // 调用到unapply
      case ApplyStudy("test", sex, age) => println(s"name:test,sex:$sex,age:$age")
      // 没有匹配结果的默认操作
      case _ => println("b")
    }
  }
}
