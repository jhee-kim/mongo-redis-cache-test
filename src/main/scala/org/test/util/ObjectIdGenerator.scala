package org.test.util


import java.util.Date
import scala.util.Random

object ObjectIdGenerator {

  def apply(): ObjectIdGenerator = new ObjectIdGenerator()
}

class ObjectIdGenerator {
  val converter = new ObjectIdConverter()

  private var cnt: String = {
    val arr = Array.ofDim[Byte](3)
    new Random().nextBytes(arr)
    arr.map("%02x".format(_)).mkString
  }

  private def inc(): Unit = {
    cnt = String.format(s"%${6}s", f"${Integer.parseInt(cnt, 16) + 1}%x").replace(" ", "0").takeRight(6)
  }

  def generate(ty: Int, pi: Option[String]): String = synchronized {
    //log.info(s"cnt : $cnt")
    inc()
    f"T$ty%05d-P${if (pi.isDefined) pi.get else ""}-${converter.encode64(new Date().getTime)}${cnt}"
  }

}