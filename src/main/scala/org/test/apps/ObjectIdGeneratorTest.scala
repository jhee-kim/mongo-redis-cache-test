package org.test.apps

import org.slf4j.LoggerFactory
import org.test.util.ObjectIdGenerator

import java.util.Date
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

object ObjectIdGeneratorTest extends App {
  private final val log = LoggerFactory.getLogger(this.getClass.getName)
  private final val THREAD_POOL_SIZE = 64

  val threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE)

  private val latch = new CountDownLatch(THREAD_POOL_SIZE)

  val generator = new ObjectIdGenerator()
  log.info(s"oid : ${generator.generate(4, Some("GUuLLtI10C2"))}")
  log.info(s"oid : ${generator.generate(4, None)}")
/*
  for (_ <- 1 to THREAD_POOL_SIZE) {
    threadPool.submit(new Runnable {
      override def run(): Unit = {
        for (_ <- 1 to 1000) {
          val id = generator.generate(4, Some("GUuLLtI10C2"))
          if (contains(id.getValue)) log.warn(s"ID 중복 : ${id}")
          else {
            add(id.getValue)
            log.info(s"oid : ${id}")
          }
          //Thread.sleep(10)
        }
        latch.countDown()
      }
    })

    Thread.sleep(13)
  }*/
  val startTime = new Date().getTime

  latch.await(600000, TimeUnit.MILLISECONDS)

  log.info(s"Finish : ${new Date().getTime - startTime}")
  threadPool.shutdown()
}
