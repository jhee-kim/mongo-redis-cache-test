package org.test.apps

import org.slf4j.LoggerFactory
import org.test.util.ObjectIdConverter

object ObjectIdConverterTest extends App {
  private final val log = LoggerFactory.getLogger(this.getClass.getName)

  val converter = new ObjectIdConverter()

/*  val time = new Date().getTime
  log.info(s"encode : ${time}")

  val enc = converter.encode64(time)
  log.info(s"encode : ${enc}")

  val dec = converter.decode64(enc)
  log.info(s"decode : ${dec}")*/


  val dec = converter.decode64("GUuLLtI10C2")
  log.info(s"decode : ${dec}")
  log.info(s"encode 16 : ${dec.toHexString}")

  val enc = converter.encode64(dec)
  log.info(s"encode : ${enc}")

  val teststr = "g//////////"
  val test = converter.decode64(teststr)
  log.info(s"decode : ${test}")

  val testenc = converter.encode64(test)
  log.info(s"decode : ${testenc}")
}
