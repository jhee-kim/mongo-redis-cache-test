package org.test.util

import org.slf4j.LoggerFactory

class ObjectIdConverter {
  private final val log = LoggerFactory.getLogger(this.getClass.getName)

  private final val base64Digits = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
  private final val base85Digits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!#$%&()*=-;<=>?@^_`{|}~"

  def encode64(value: Long): String = {
    var decValue = value
    val builder = new StringBuilder()
    while (decValue > 0) {
      builder.insert(0, base64Digits.charAt((decValue % 64).toInt))
      decValue /= 64
    }
    builder.toString()
  }

  def decode64(value: String): Long = {
    var decValue: Long = 0
    for (i <- 0 until value.length) {
      val v = base64Digits.indexOf(value.charAt(value.length - i - 1)) * scala.math.pow(64, i).toLong
      if (v < 0) return -1
      decValue = decValue + v
      if (decValue < v || decValue > Long.MaxValue) return -1
    }
    decValue
  }
}
