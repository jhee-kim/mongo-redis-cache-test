package org.test.dm.redis

import org.redisson.Redisson
import org.redisson.api.{RMapCache, RedissonClient}
import org.redisson.config.Config
import org.slf4j.LoggerFactory
import org.test.dm.redis.RedisDataManager.DEFAULT_TTL_TIME_SEC
import org.test.dm.redis.RedisCacheDataManager.DEFAULT_CACHE_NAME

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters.mapAsScalaMapConverter

object RedisCacheDataManager {
  private final val DEFAULT_CACHE_NAME = "db_cache"

  def apply(): RedisCacheDataManager = new RedisCacheDataManager()
}

class RedisCacheDataManager {
  private final val log = LoggerFactory.getLogger(this.getClass.getName)

  private final val config = new Config()

  private var _client: Option[RedissonClient] = None
  private var _cache: Option[RMapCache[String, Any]] = None

  private def client: RedissonClient = _client.getOrElse(throw new Exception("Redis initialize failed."))
  private def cache: RMapCache[String, Any] = _cache.getOrElse(throw new Exception("Redis cache initialize failed."))

  def init(): Unit = {
    config
      .useSingleServer()
      .setAddress("redis://localhost:6379")
      .setDatabase(2)

    _client = Option(Redisson.create(config))
    _cache = Option(client.getMapCache[String, Any](DEFAULT_CACHE_NAME))
  }

  def release(): Unit = {
    _client match {
      case Some(client) => client.shutdown(); _client = None
      case None => // not to do
    }
  }

  private class TypeCastingFailedException(val msg: String) extends RuntimeException

  private def toList[T](value: Any): List[T] =
    try {
      value match {
        case Some(v) => v.asInstanceOf[List[T]]
        case None => throw new RuntimeException("None type value")
        case any => any.asInstanceOf[List[T]]
      }
    } catch {
      case e: Exception => e.printStackTrace(); throw new TypeCastingFailedException(s"toList Failed : ${e.getMessage}");
    }

  private def toMap[T](value: Any): Map[String, T] =
    try {
      value match {
        case Some(v) => v match {
          case m: Map[String, T] => m
          case o => o.asInstanceOf[java.util.Map[String, T]].asScala.toMap
        }
        case None => throw new RuntimeException("None type value")
        case any => any.asInstanceOf[java.util.Map[String, T]].asScala.toMap
      }
    } catch {
      case e: Exception => throw new TypeCastingFailedException(s"toMap Failed : ${e.getMessage}");
    }

  def setTTL[T](key: String, ttl: Long = DEFAULT_TTL_TIME_SEC, timeunit: TimeUnit = TimeUnit.SECONDS): Boolean =
    try {
      val value = get[T](key)
      if (value.isDefined) set(key, value.get, ttl, timeunit).isDefined
      else false
    } catch {
      case e: Exception => throw new TypeCastingFailedException(s"TTL set Failed : ${e.getMessage}");
    }

  def set[T](key: String, value: T, ttl: Long = DEFAULT_TTL_TIME_SEC, timeunit: TimeUnit = TimeUnit.SECONDS): Option[T] =
    Option(cache.put(key, value, ttl, timeunit).asInstanceOf[T])

  def get[T](key: String): Option[T] = if (exists(key)) Option(cache.get(key).asInstanceOf[T]) else None


  def del(key: String): Any = cache.remove(key)

  def exists(key: String): Boolean = cache.containsKey(key)

  def size(): Int = cache.size()

  def clearExpire(): Boolean = cache.clearExpire()

  def lpush[T](key: String, value: T): Option[List[T]] =
    try {
      if (exists(key))  set(key, value +: toList[T](get(key)))
      else              set(key, List(value))
    } catch {
      case e: Exception => log.warn(s"LPUSH failed : ${e.getMessage}"); e.printStackTrace(); None
    }

  def lpushAll[T](key: String, value: List[T]): Option[List[T]] =
    try {
      if (exists(key)) set(key, value ++ toList[T](get(key)))
      else set(key, value)
    } catch {
      case e: Exception => log.warn(s"LPUSHALL failed : ${e.getMessage}"); None
    }

  def lpushWithTrim[T](key: String, value: T, limit: Int): Option[List[T]] =
    try {
      if (exists(key)) {
        val list = toList[T](get(key))
        set(key, value +: list.take(math.min(limit - 2, list.length)))
      }
      else set(key, List(value))
    } catch {
      case e: Exception => log.warn(s"LPUSH with trim failed : ${e.getMessage}"); None
    }

  def lpushAllWithTrim[T](key: String, value: List[T], limit: Int): Option[List[T]] =
    try {
      if (exists(key)) {
        val list = toList[T](get(key))
        set(key, value ++ list.take(math.min(limit - value.length - 1, list.length)))
      }
      else set(key, value)
    } catch {
      case e: Exception => log.warn(s"LPUSHALL with trim failed : ${e.getMessage}"); None
    }

  def rpush[T](key: String, value: T): Option[List[T]] =
    try {
      if (exists(key))  set(key, toList[T](get(key)) :+ value)
      else              set(key, List(value))
    } catch {
      case e: Exception => log.warn(s"RPUSH failed : ${e.getMessage}"); None
    }

  def rpushWithTrim[T](key: String, value: T, limit: Int): Option[List[T]] =
    try {
      if (exists(key)) {
        val list = toList[T](get(key))
        set(key, list.takeRight(math.min(limit - 2, list.length)) :+ value)
      }
      else set(key, List(value))
    } catch {
      case e: Exception => log.warn(s"RPUSH failed : ${e.getMessage}"); None
    }

  def rpushAll[T](key: String, value: List[T]): Option[List[T]] =
    try {
      if (exists(key)) set(key, toList[T](get(key)) ++ value)
      else set(key, value)
    } catch {
      case e: Exception => log.warn(s"RPUSHALL failed : ${e.getMessage}"); None
    }

  def rpushAllWithTrim[T](key: String, value: List[T], limit: Int): Option[List[T]] =
    try {
      if (exists(key)) {
        val list = toList[T](get(key))
        set(key, list.takeRight(math.min(limit - value.length - 1, list.length)) ++ value)
      }
      else set(key, value)
    } catch {
      case e: Exception => log.warn(s"RPUSH failed : ${e.getMessage}"); None
    }

  def trim[T](key: String, start: Int = 0, stop: Int = 9): Option[List[T]] =
    try {
      if (exists(key)) {
        val list = toList[T](get(key))
        set(key, list.slice(start, math.min(stop, list.size - 1)))
      }
      else None
    } catch {
      case e: Exception => log.warn(s"List trim failed : ${e.getMessage}"); None
    }

  def hset[T](key: String, field: String, value: T): Option[Map[String, T]] =
    try {
      var res: Option[Map[String, T]] = None
      if (exists(key))  res = set(key, toMap[T](get(key)) + (field -> value))
      else              res = set(key, Map[String, T](field -> value))
      res
    } catch {
      case e: Exception => log.warn(s"Hash Set failed : ${e.getMessage}"); None
    }

  def hget[T](key: String, field: String): Option[T] =
    try {
      if (exists(key))  toMap[T](get(key)).get(field)
      else              None
    } catch {
      case e: Exception => log.warn(s"Hash get failed : ${e.getMessage}"); None
    }

  // 증감 후 결과값 반환
  // -1 : 오류
  def hincrby(key: String, field: String, increment: Int): Long = {
    try {
      val oldValue = hget[Int](key, field)
      var res: Int = 0
      if (oldValue.isDefined) res = oldValue.get.+(increment)
      else                    res = increment
      if (res >= 0) hset(key, field, res)
      else          throw new RuntimeException(s"Invalid value - $res")

      res.toLong
    } catch {
      case e: Exception => log.warn(s"Hash increase failed : ${e.getMessage}"); -1
    }
  }
}
