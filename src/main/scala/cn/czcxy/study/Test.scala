package cn.czcxy.study

import cn.czcxy.study.utils.RedisUtil
import redis.clients.jedis.JedisPool

object Test {
  def main(args: Array[String]): Unit = {
    val pool: JedisPool = RedisUtil.getInstance()
    pool.getResource.hset("a:b", "4", "1")
  }
}
