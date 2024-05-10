package org.test.apps

import org.slf4j.LoggerFactory
import org.test.dm.redis.RedisDataManager

object RedisTest02 extends App {
  private final val log = LoggerFactory.getLogger(this.getClass.getName)

  val dar = RedisDataManager()
  dar.init()

  log.info(s"keys : ${dar.keys("*").mkString("[", ",", "]")}")

  log.info(s"key exists asdf : ${dar.exists("asdf").toString}")
  log.info(s"key exists abcd : ${dar.exists("abcd").toString}")


  dar.release()
}
