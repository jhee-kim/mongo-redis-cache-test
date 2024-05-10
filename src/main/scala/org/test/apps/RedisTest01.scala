package org.test.apps

import org.slf4j.LoggerFactory
import org.test.dm.redis.RedisDataManager

object RedisTest01 extends App {
  private final val log = LoggerFactory.getLogger(this.getClass.getName)
  private final val THREAD_POOL_SIZE = 128

  val dar = RedisDataManager()
  dar.init()

  dar.set("dar", "test")
  log.info(dar.get[String]("dar"))

  dar.set("darc", "codectest")
  log.info(dar.get[String]("darc"))

  dar.set("cnttest", 0)
  log.info(dar.get[Long]("cnttest").toString)

  dar.set("cnt", 0)

/*  val threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE)

  val latch = new CountDownLatch(THREAD_POOL_SIZE)

  for (_ <- 1 to THREAD_POOL_SIZE) {
    threadPool.submit(new Runnable {
      override def run(): Unit = {
        for (_ <- 1 to 1000) {
          dar.incr("cnt")
          latch.countDown()
        }
      }
    })
  }

  latch.await()

  Thread.sleep(10000)

  log.info(s"cnt : ${dar.getLong("cnt")}")*/


  //dar.hset("container01", "_1", 0)


  log.info(s"hset created")

  Thread.sleep(3000)


  dar.hincrby("container01", "_1", 1)
  dar.hincrby("container01", "_2", 1)

  Thread.sleep(3000)

  log.info(s"hget 1 : ${dar.hget("container01", "_1")}")
  log.info(s"hget 2 : ${dar.hget("container01", "_2")}")
  log.info(s"hget 3 : ${dar.hget("container01", "_3")}")

  dar.del("dar")
  dar.del("cnt")
  dar.del("cntLock")

  Thread.sleep(3000)

  dar.release()

  //threadPool.shutdown()
}
