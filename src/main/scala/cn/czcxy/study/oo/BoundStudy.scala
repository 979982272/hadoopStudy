package cn.czcxy.study.oo

import ContextPredefStudy.viewBoundChoose
import ContextPredefStudy.OrderContextBound
/**
  * 泛型:
  * [B <: A]  UpperBound 上界: B类型的上界是A类型,即B类型的父类是A类型
  * [B >: A]  LowerBound 下界: B类型的下界是A类型,即B类型的子类是A类型
  * [B <% A]  ViewBound  表示B类型要转换为A类型，需要一个隐式转换函数
  * [B : A]   ContextBound 需要一个隐式转换的值
  * [-A,+B]
  * [-A] 逆变，作为参数类型。如果A是T的子类，那么C[T]是C[A]的子类
  * [+B] 协变，作为返回类型。如果B是T的子类，那么C[B]是C[T]的子类
  */
class BoundStudy {

}

/**
  * 泛型-上界
  *
  * @tparam T
  */
class UpperBoundDemo[T <: Comparable[T]] {
  def choose(v1: T, v2: T): T = if (v1.compareTo(v2) > 0) v1 else v2
}

object UpperBoundDemo {
  def main(args: Array[String]): Unit = {
    val upperBound = new UpperBoundDemo[UpperBoundPeople]
    val v1 = new UpperBoundPeople("test01", 16)
    val v2 = new UpperBoundPeople("test02", 26)
    println(upperBound.choose(v1, v2))
  }
}

class UpperBoundPeople(val name: String, val age: Int) extends Comparable[UpperBoundPeople] {
  // 实现对比接口函数
  override def compareTo(o: UpperBoundPeople): Int = this.age - o.age

  override def toString: String = s"name:$name,age:$age"
}

/**
  * 泛型 viewbound 类型，需要一个隐式转换函数
  * ContextPredefStudy.viewBoundChoose
  *
  * @param ev$1
  * @tparam T
  */
class ViewBoundDemo[T <% Ordered[T]] {
  def choose(v1: T, v2: T): T = if (v1 > v2) v1 else v2
}

object ViewBoundDemo {
  def main(args: Array[String]): Unit = {
    val viewBound = new ViewBoundDemo[ViewBoundPeople]
    val v1 = new ViewBoundPeople("test", 11)
    val v2 = new ViewBoundPeople("test01", 15)
    println(viewBound.choose(v1, v2))
  }
}

class ViewBoundPeople(val name: String, val age: Int) {
  override def toString: String = s"name:$name,age:$age"
}

/**
  * 泛型 需要一个隐式转换的值
  *
  * @param ev$1
  * @tparam T
  */
class ContextBoundDemo[T: Ordering] {
  def choose(v1: T, v2: T): T = {
    val ord: Ordering[T] = implicitly[Ordering[T]]
    if (ord.gt(v1, v2)) v1 else v2
  }
}

object ContextBoundDemo {
  def main(args: Array[String]): Unit = {
    val contextBound = new ContextBoundDemo[ContextBoundPeople]
    val v1 = new ContextBoundPeople("test", 11)
    val v2 = new ContextBoundPeople("test01", 15)
    println(contextBound.choose(v1, v2))
  }
}

class ContextBoundPeople(val name: String, val age: Int) {
  override def toString: String = s"name:$name,age:$age"
}


