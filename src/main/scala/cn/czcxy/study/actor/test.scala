package cn.czcxy.study.actor

import akka.actor.typed.ActorSystem

object test {
  def main(args: Array[String]): Unit = {
    println(1)
    val testSystem = ActorSystem(MainActor(), "test")
    testSystem ! "start"
  }
}
