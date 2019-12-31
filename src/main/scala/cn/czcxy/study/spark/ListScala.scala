package cn.czcxy.study.spark

object ListScala {
  def main(args: Array[String]): Unit = {
    // 空列表
    var empty: List[Nothing] = List()
    var emptyList = Nil
    // 整形列表
    var intList: List[Int] = List(1, 2, 3)
    var result = List.concat(intList, getInItList())
    // 在list开头插入元素
    result = 4 +: result
    // 在list最后插入元素
    result = result :+ 5
    println(result)
    
  }

  /**
    * list
    */
  def getInItList(): List[Int] = {
    return 1 :: (2 :: (3 :: Nil))
  }

}
