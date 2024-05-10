package org.test.apps

import org.slf4j.LoggerFactory
import org.test.dm.redis.RedisDataManager

object RedisTest03 extends App {
  private final val log = LoggerFactory.getLogger(this.getClass.getName)

  val dar = RedisDataManager()
  dar.init()

  // 리스트 추가 및 값 추가
  dar.lpush("listTest", "0")
  dar.lpush("listTest", "1")

  log.info(dar.lgetAll("listTest").mkString(","))

  Thread.sleep(5000)

  // 리스트 자르기
  dar.trim("listTest", 0, 1)

  log.info(dar.lgetAll("listTest").mkString(","))

  Thread.sleep(5000)

  // 리스트 삭제
  dar.del("listTest")

  log.info(dar.lgetAll("listTest").mkString(","))

  Thread.sleep(5000)

  dar.release()
}
