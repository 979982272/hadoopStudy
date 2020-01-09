package cn.czcxy.study.spark.oo

import java.util.{Comparator, function}

import scala.collection.immutable.StringOps

/**
  * 隐式属性
  */
object ContextPredefStudy {
  implicit val a: Int = 6

  // string 隐式转int
  implicit def str2Int(a: String) = new StringOps(a).toInt

  // 读取文件
  implicit def filePathToRichFile(filePath: String) = new RichFileScala(filePath)

  // 比较类型，返回年龄大的
  implicit object OrderGril extends Ordering[Girl] {
    override def compare(x: Girl, y: Girl): Int = if (x.age > y.age) 1 else -1
  }

  // viewbound 隐式转换
  implicit val viewBoundChoose = (g: ViewBoundPeople) => new Ordered[ViewBoundPeople] {
    override def compare(that: ViewBoundPeople): Int = g.age - that.age
  }

  // 比较类型，返回年龄大的
  implicit object OrderContextBound extends Ordering[ContextBoundPeople] {
    override def compare(x: ContextBoundPeople, y: ContextBoundPeople): Int = if (x.age > y.age) 1 else -1
  }

}
