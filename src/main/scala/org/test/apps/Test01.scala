package org.test.apps

import org.slf4j.LoggerFactory
import org.test.dm.mongo.MongoDataManager

import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object Test01 extends App {

  private final val log = LoggerFactory.getLogger(this.getClass.getName)

  private final val MAX_LOOP_NUMBER = 10
  private final val THREAD_SLEEP_MS = 500

  private val latch = new CountDownLatch(MAX_LOOP_NUMBER)

  val da = MongoDataManager()

  da.init()


  for (i <- 1 to MAX_LOOP_NUMBER) {
    log.info(s"Start count : $i")
    da.countDocuments(4, "bSmSvQZP0C2")
      .onComplete({
        case Success(value) =>
          log.info(s"CountDocument [ $i ] : $value")
          latch.countDown()
        case Failure(exception) =>
          log.warn(s"Failed [ $i ] : ${exception.getMessage}")
          latch.countDown()
        case _ =>
          log.warn(s"Unknown Error")
          latch.countDown()
      })
    Thread.sleep(THREAD_SLEEP_MS)
  }

  latch.await(THREAD_SLEEP_MS * MAX_LOOP_NUMBER + 30000, TimeUnit.MILLISECONDS)

  da.release()
}
