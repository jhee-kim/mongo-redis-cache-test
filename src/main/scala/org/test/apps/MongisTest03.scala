package org.test.apps

import org.slf4j.LoggerFactory
import org.test.dm.mongis.{MongisDataManager, MongisCacheDataManager}

import java.util.concurrent.CountDownLatch
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object MongisTest03 extends App {
  private final val log = LoggerFactory.getLogger(this.getClass.getName)

  private final val MAX_LOOP_NUMBER = 2000
  private final val THREAD_SLEEP_MS = 5000


  private val latch = new CountDownLatch(1)


  val dam = MongisCacheDataManager()
  dam.init()

/*  new Thread() {
    override def run(): Unit = {
      Thread.sleep(1000 * 60 * 30)
      latch.countDown()
    }
  }.start()*/

  new Thread() {
    override def run(): Unit = {
      for (i <- 1 to MAX_LOOP_NUMBER) {
        log.info(s"Start count : $i")
        if (i % 2 == 0) {
          dam.discoveryDashboard("data", 4, 10)
            .onComplete({
              case Success(value) =>
                log.info(s"discovery [ $i ] : $value")
              case Failure(exception) =>
                log.warn(s"Failed [ $i ] : ${exception.getMessage}")
              case _ =>
                log.warn(s"Unknown Error")
            })
        } else {
          dam.countDocuments(4, "GUuLLtI10C2")
            .onComplete({
              case Success(value) =>
                log.info(s"countDocument [ $i ] : $value")
              case Failure(exception) =>
                log.warn(s"Failed [ $i ] : ${exception.getMessage}")
              case _ =>
                log.warn(s"Unknown Error")
            })
        }
        Thread.sleep(THREAD_SLEEP_MS)
      }
      latch.countDown()
    }
  }.start()

  latch.await()

  log.info("Test finish.")
  dam.release()
}
