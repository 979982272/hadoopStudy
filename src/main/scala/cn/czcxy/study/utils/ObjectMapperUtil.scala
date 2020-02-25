package cn.czcxy.study.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object ObjectMapperUtil {
  // @transient 传输时不被序列化
  @transient private var instance: ObjectMapper = _

  /**
    * 获取单例对象
    *
    * @return
    */
  def getInstance(): ObjectMapper = {
    if (null == instance) {
      instance = new ObjectMapper()
      instance.registerModule(DefaultScalaModule)
    }
    instance
  }
}
