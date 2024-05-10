package org.test.apps

import org.slf4j.LoggerFactory
import org.test.dm.redis.{RedisDataManager, RedisCacheDataManager}

object RedisTest04 extends App {
  private final val log = LoggerFactory.getLogger(this.getClass.getName)

  val darc = RedisCacheDataManager()
  darc.init()

  // 리스트 추가 및 값 추가
  darc.lpush[String]("listTest", "0")
  darc.lpush[String]("listTest", "1")

  log.info(darc.get("listTest").mkString(","))

  Thread.sleep(5000)

  // 리스트 자르기.
  darc.trim("listTest", 0, 1)

  log.info(darc.get("listTest").mkString(","))

  Thread.sleep(5000)

  // 리스트 삭제
  darc.del("listTest")

  log.info(darc.get("listTest").mkString(","))

  Thread.sleep(5000)

  darc.release()
}
