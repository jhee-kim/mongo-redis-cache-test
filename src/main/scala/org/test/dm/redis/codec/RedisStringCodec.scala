package org.test.dm.redis.codec

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.util.CharsetUtil
import org.redisson.client.codec.BaseCodec
import org.redisson.client.handler.State
import org.redisson.client.protocol.{Decoder, Encoder}

class RedisStringCodec extends BaseCodec {
  override def getValueDecoder: Decoder[AnyRef] = {
    (buf: ByteBuf, state: State) => {
      try {
        val bytes = Array.ofDim[Byte](buf.readableBytes())
        buf.readBytes(bytes)
        new String(bytes, CharsetUtil.UTF_8)
      } catch {
        case e: Exception => e.printStackTrace(); throw new RuntimeException(s"Deserialize failed : ${e.getMessage}")
      }
    }
  }

  override def getValueEncoder: Encoder = {
    case str: String =>
      val sb: Array[Byte] = str.getBytes(CharsetUtil.UTF_8)
      Unpooled.buffer(sb.length).writeBytes(sb)
    case e => throw new RuntimeException(s"Unsupported type : ${e.getClass.getName}")
  }
}
