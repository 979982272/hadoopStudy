package cn.czcxy.study.oo

import scala.io.Source
import ContextPredefStudy._

// 读取某个文件
class RichFileScala(val filePath: String) {
  def read(): String = {
    Source.fromFile(filePath).mkString
  }
}

object RichFileScala {
  def main(args: Array[String]): Unit = {
    // 显式调用Richfile的read方法
    val filePath = "C:\\test.log"
    var fileInfo = new RichFileScala(filePath).read()
    println(fileInfo)
    // 隐式调用
    fileInfo = filePath.read()
    println(fileInfo)
  }
}

/**
  * 对象类
  *
  * @param name
  * @param age
  */
class Girl(val name: String, val age: Int) {
  override def toString: String = s"name=$name,age=$age"
}

/**
  * 撰写一个类，，可以比较出年纪大的
  *
  * @param v1
  * @param v2
  * @tparam T
  */
class YearOld[T: Ordering](val v1: T, val v2: T) {
  /**
    * 选择方法
    *
    * @param ord 使用隐形转化，拿到cn.czcxy.study.spark.oo.ContextScala.OrderGril
    * @return
    */
  def choose()(implicit ord: Ordering[T]) = if (ord.gt(v1, v2)) v1 else v2
}

object YearOld {
  def main(args: Array[String]): Unit = {
    val v1: Girl = new Girl("test01", 26)
    val v2: Girl = new Girl("test02", 27)
    val yearOld = new YearOld(v1, v2)
    println(yearOld.choose())
  }
}