package org.test.dm.redis

import org.redisson.Redisson
import org.redisson.api.{RBucket, RList, RMap, RedissonClient}
import org.redisson.client.codec.{ByteArrayCodec, Codec, DoubleCodec, IntegerCodec, LongCodec, StringCodec}
import org.redisson.codec.JsonJacksonCodec
import org.redisson.config.Config
import org.slf4j.LoggerFactory
import RedisDataManager.{DEFAULT_TIMEOUT_MS, DEFAULT_TTL_TIME_SEC}

import java.time.Duration
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object RedisDataManager {
  final val DEFAULT_TIMEOUT_MS = 5000
  final val DEFAULT_LOCK_TIMEOUT_MS = 10000
  final val DEFAULT_TTL_TIME_SEC = 60

  def apply(): RedisDataManager = new RedisDataManager()
}

class RedisDataManager {
  private final val log = LoggerFactory.getLogger(this.getClass.getName)

  private final val config = new Config()

  private var _client: Option[RedissonClient] = None

  private def client: RedissonClient = _client.getOrElse(throw new Exception("Redis initialize failed."))

  def init(): Unit = {
    config
      .useSingleServer()
      .setAddress("redis://10.10.30.78:6379")
      .setDatabase(2)

    _client = Option(Redisson.create(config))
  }

  def release(): Unit = {
    _client match {
      case Some(client) => client.shutdown(); _client = None
      case None => // not to do
    }
  }

  def initTTL(keys: Seq[String]): Unit = {
    keys.foreach(key => {
      val bucket = client.getBucket(key)
      if (bucket.isExists) bucket.expire(Duration.ZERO) else false
    })
  }

  def setTTL(key: String, timeout: Duration = Duration.ofSeconds(DEFAULT_TTL_TIME_SEC)): Boolean = {
    val bucket = client.getBucket(key)
    if (bucket.isExists) {
      bucket.remainTimeToLive() match {
        case ttl if ttl > 0 =>
          log.info(s"[ $key ] TTL exist : $ttl")
        case _ =>
          log.info(s"[ $key ] TTL NOT exist")
      }
      bucket.expire(timeout)
    }
    else false
  }

  private def getStringBucket(key: String): RBucket[String] = client.getBucket[String](key, StringCodec.INSTANCE)
  private def getList[T](key: String): RList[T] = client.getList(key, JsonJacksonCodec.INSTANCE)
  private def getMap[T](key: String): RMap[String, T] = client.getMap(key, JsonJacksonCodec.INSTANCE)

  def del(key: String): Long = client.getKeys.delete(key)

  def set[T](key: String, value: T): Unit = {
    var codec: Codec = null
    codec = value match {
      case _: Int => IntegerCodec.INSTANCE
      case _: Long => LongCodec.INSTANCE
      case _: Double => DoubleCodec.INSTANCE
      case _: String => StringCodec.INSTANCE
      case _ => ByteArrayCodec.INSTANCE
    }
    log.info(s"set codec : ${codec.toString}")
    client.getBucket[T](key, codec).set(value)
  }

  def get[T: ClassTag](key: String): T = {
    log.info(s"T: ${implicitly[ClassTag[T]].runtimeClass.getName}")
    var codec: Codec = null
    codec = implicitly[ClassTag[T]] match {
      case long if long.runtimeClass == classOf[Int] => IntegerCodec.INSTANCE
      case long if long.runtimeClass == classOf[Long] => LongCodec.INSTANCE
      case double if double.runtimeClass == classOf[Double] => DoubleCodec.INSTANCE
      case str if str.runtimeClass == classOf[String] => StringCodec.INSTANCE
      case _ => ByteArrayCodec.INSTANCE
    }
    log.info(s"get codec : ${codec.toString}")
    client.getBucket[T](key, codec).get()
  }
  /*
    def getString(key: String): String = getStringBucket(key).get()
    def getLong(key: String): Long = client.getAtomicLong(key).get()*/

  def exists(key: String): Boolean =
    if (client.getKeys.countExistsAsync(key).get(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS) > 0) true else false

  def keys(pattern: String): Seq[String] = client.getKeys.getKeysByPattern(pattern).asScala.toSeq

  def incr(key: String): Long = client.getAtomicLong(key).incrementAndGet()
  def decr(key: String): Long = client.getAtomicLong(key).decrementAndGet()

  def lpush[T](key: String, value: T): Unit = getList[T](key).add(0, value)

  def lpushAll[T](key: String, values: Seq[T]): Unit = getList[T](key).addAll(0, values.asJava)

  def rpush[T](key: String, value: T): Unit = getList[T](key).add(value)

  def rpushAll[T](key: String, values: Seq[T]): Unit = getList[T](key).addAll(values.asJava)

  def lgetAll[T](key: String): Option[List[T]] = Option(getList[T](key).readAll().asScala.toList)

  def llen[T](key: String): Int = getList[T](key).size()

  def trim[T](key: String, start: Int = 0, stop: Int = 9): Unit = {
    val list = getList[T](key)
    list.trim(start, math.min(stop, list.size() - 1))
  }

  def hset[T](key: String, field: String, value: T): T = getMap(key).put(field, value)
  //@throws[Exception] def hmset[T](key: String, fv: Map[String, T]): Unit = getMap(keyWithPrefix(key)).putAll(fv.asJava)
  def hget[T](key: String, field: String): Option[T] = Option(getMap(key).get(field))
  //@throws[Exception] def hgetall(key: String)
  def hincrby(key: String, field: String, increment: Long): Long = getMap(key).addAndGet(field, increment)
  def hincrbyFloat(key: String, field: String, increment: Double): Double = getMap(key).addAndGet(field, increment)
}
