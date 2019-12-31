package cn.czcxy.study.spark

object MapScala {
  def main(args: Array[String]): Unit = {
    var map: Map[String, Int] = Map("a" -> 1, "b" -> 2)
    println(map)
    // 给map添加元素
    map += ("c" -> 3)
    println(map)
    // 输入所有的map值
    map.keys.foreach(key => {
      println(map(key))
    })
  }
}
