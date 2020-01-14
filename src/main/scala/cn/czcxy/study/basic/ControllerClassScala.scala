package cn.czcxy.study.basic

class ControllerClassScala {
  def controller(met: (Int, Int) => Int): Int = {
    return met(10, 5)
  }
}

