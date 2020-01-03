package cn.czcxy.study.spark

class ControllerClassScala {
  def controller(met: (Int, Int) => Int): Int = {
    return met(10, 5)
  }
}

