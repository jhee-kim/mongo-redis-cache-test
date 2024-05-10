package org.test.apps

import org.slf4j.LoggerFactory
import org.test.dm.mongis.MongisDataManager
import org.test.dm.mongo.MongoDataManager

import java.util.Date
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object Test03 extends App {

  private final val log = LoggerFactory.getLogger(this.getClass.getName)

  private final val MAX_LOOP_NUMBER = 20
  private final val THREAD_SLEEP_MS = 10000

  private val latch = new CountDownLatch(MAX_LOOP_NUMBER)

  val da = MongisDataManager()

  da.init()


  for (j <- 1 to 10000) {
    log.info(s"Start count : $j")
    for (i <- 1 to MAX_LOOP_NUMBER) {
      val startTime = new Date().getTime
      //log.info(s"*** Start : $startTime ***")
      da.discovery("data", 4, 0, 10, "ct,desc")
        .onComplete({
          case Success(value) =>
            log.info(s"*** Finish $j-$i : ${new Date().getTime - startTime} ***")
            log.info(s"Discovery [ $i ]\n$value")
            latch.countDown()
          case Failure(exception) =>
            log.info(s"*** Finish $j-$i : ${new Date().getTime - startTime} ***")
            log.warn(s"Failed [ $i ] : ${exception.getMessage}")
            latch.countDown()
          case _ =>
            log.info(s"*** Finish $j-$i : ${new Date().getTime - startTime} ***")
            log.warn(s"Unknown Error")
            latch.countDown()
        })
    }
    Thread.sleep(THREAD_SLEEP_MS)
  }

  latch.await(THREAD_SLEEP_MS * MAX_LOOP_NUMBER + 30000, TimeUnit.MILLISECONDS)

  da.release()
}
