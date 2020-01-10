package cn.czcxy.study.spark.actor

import akka.actor.Actor
import akka.actor.typed.{ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}

class ActorDemo1(context: ActorContext[String]) extends AbstractBehavior[String](context) {
  override def onMessage(msg: String): Behavior[String] =
    msg match {
      case "printit" =>
        val s = context.spawn(Behaviors.empty[String], "second-actor")

        context.stop(s)
        println(s"second:$s")

        this
    }
}

object ActorDemo1 {
  def apply(): Behavior[String] = Behaviors.setup(context => new ActorDemo1(context))
}


object MainActor {
  def apply(): Behavior[String] = Behaviors.setup(context => new MainActor(context))
}

class MainActor(context: ActorContext[String]) extends AbstractBehavior[String](context) {
  override def onMessage(msg: String): Behavior[String] =
    msg match {
      case "start" => {
        var s = context.spawn(ActorDemo1(), "first")
        println(s"first:$s")
        s ! "printit"
        this
      }
    }
}

object Application extends App {
  val testSystem = ActorSystem(MainActor(), "test")
  testSystem ! "start"
  // ! 发送异步消息 没有返回值
  // !? 同步的没有返回值 有返回值
  // !! 发送异步消息，返回值时Future[Any]
  // 用actor 编写wordcount
}