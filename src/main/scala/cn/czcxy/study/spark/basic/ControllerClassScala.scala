package cn.czcxy.study.spark.basic

class ControllerClassScala {
  def controller(met: (Int, Int) => Int): Int = {
    return met(10, 5)
  }
}

