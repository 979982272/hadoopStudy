package cn.czcxy.study.spark

/**
  * 分词计数
  */
object WordCountScala {
  def main(args: Array[String]): Unit = {
    var lines = List("hello world hello java", "hello python ", "hello java hello go hello scala")
    // 把数据切分合并成一个list
    var words = lines.flatMap(_.split(" "))
    println(words)
    // 把数据整合成一个元祖
    var wordTuples = words.map((_, 1))
    println(wordTuples)
    // 使用元组，按照key的值分组
    var wordGroup = wordTuples.groupBy(_._1)
    println(wordGroup)
    // 使用元组的mapvalue方法，会将value的值传入；调用list大小就得到单词数量
    var wordSum = wordGroup.mapValues(_.size)
    println(wordSum)
    // 排序
    var wordSort = wordSum.toList.sortBy(_._2)
    println(wordSort)
    // 倒序
    var result = wordSort.reverse
    println(result)
  }
}
