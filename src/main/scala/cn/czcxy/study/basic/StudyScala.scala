package cn.czcxy.study.basic

import scala.collection.mutable

object StudyScala {
  /**
    * scala练习
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    // 生成一个list
    var initList = List(9, 1, 2, 3, 6, 4, 8, 5, 7, 10)

    // 将initList元素中的每个元素乘以2生成新集合
    var doubleList: List[Int] = Nil
    for (index <- initList) {
      doubleList = doubleList :+ index * 2
    }

    val doubleListTwo = initList.map(_ * 2)
    println("所有元素乘以2:" + doubleList)
    println("所有元素乘以2:" + doubleListTwo)


    // 将initlist中的偶数取出来生成新集合
    var evenNumberList: List[Int] = Nil
    for (index <- initList) {
      if (index % 2 == 0) {
        evenNumberList = evenNumberList :+ index
      }
    }
    val evenNumberListTwo = initList.filter(_ % 2 == 0)

    println("所有偶数集合:" + evenNumberList)
    println("所有偶数集合:" + evenNumberListTwo)

    // 将initlist排序后生成新集合

    println("升序排序:" + initList.sortWith(asc))
    println("升序排序:" + initList.sorted)
    println("降序排序:" + initList.sortWith(desc))
    println("降序排序:" + initList.sorted.reverse)

    // 将initlist反序后生成新集合--把排序函数修改返回值
    // 直接使用toBuffer打印之后iterable会被清空；所以使用buffer
    var b: mutable.Buffer[List[Int]] = initList.grouped(4).toBuffer
    println(b)
    // 将iterable转换为list
    println(b.toList)

    // 合并两个list
    var lista = List(1, 2, 3)
    var listb = List(4, 5, 6)
    println(List.concat(lista, listb))

    // 求和
    val count = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    println(count.sum)
    // 并行计算求和
    println(count.par.sum)
    // 按照特定顺序聚合 代表(_ + _) 第一个的值加上第二个的
    println(count.reduce(_ + _))
    // 并行按照特定顺序聚合
    println(count.par.reduce(_ + _))
    // 折叠，有初始值 无特定顺序 初始值为0
    println(count.fold(0)(_ + _))

    // 聚合相加;flatten 把多个list合并为一个
    val lists: List[List[Int]] = List(List(1, 2, 3), List(4, 5, 6), List(7, 8))
    println(lists.flatten.reduce(_ + _))
    // 直接聚合，指定初始化值为0；(_ + _.sum)代表用0+第一个list.sum;(_ + _)代表第一个list的结果和第二个的结果相加
    println(lists.aggregate(0)(_ + _.sum, _ + _))
    // 传入函数的写法不同;_ 的顺序代表对应的参数
    lists.aggregate(0)((a: Int, b: List[Int]) => a + b.sum, tes02)

    // 并集，交集，差集
    val list01 = List(1, 2, 3, 4, 5)
    val list02 = List(4, 5, 6, 7, 8)
    // 并集
    println(list01 union list02)
    // 交集
    println(list01 intersect list02)
    // 差集
    println(list01 diff list02)
  }

  var tes01 = (a: Int, b: List[Int]) => {
    a + b.sum
  }

  var tes02 = (a: Int, b: Int) => {
    a + b
  }


  /**
    *
    * @param a
    * @param b
    * @param orderType true 升序；false 降序
    * @return
    */
  def orderBy(a: Int, b: Int, orderType: Boolean): Boolean = {
    var result = orderType
    if (a > b) {
      result = !orderType
    }
    return result
  }

  /**
    * 降序排序
    *
    * @param a
    * @param b
    * @return
    */
  def asc(a: Int, b: Int) = orderBy(a, b, true)

  /**
    * 升序排序
    *
    * @param a
    * @param b
    * @return
    */
  def desc(a: Int, b: Int) = orderBy(a, b, false)
}
