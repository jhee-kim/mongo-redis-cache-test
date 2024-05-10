package org.test.dm.mongis

import com.mongodb.client.model.changestream.OperationType
import org.bson._
import org.mongodb.scala.Observer
import org.mongodb.scala.bson.{BsonInt32, BsonObjectId, BsonValue, Document}
import org.mongodb.scala.model.changestream.{ChangeStreamDocument, FullDocument}
import org.mongodb.scala.model.{Filters, Sorts}
import org.slf4j.LoggerFactory
import org.test.dm.mongis.MongisCacheDataManager.{DEFAULT_DASHBOARD_LIMIT_SIZE, hashKey, listKey}
import org.test.dm.mongo.MongoDataManager
import org.test.dm.redis.{RedisDataManager, RedisCacheDataManager}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object MongisCacheDataManager {
  private final val DEFAULT_PREFIX_ONEM2MID_KEY = "_oneM2M."
  private final val DEFAULT_PREFIX_LIST_KEY = "li."

  private final val DEFAULT_DASHBOARD_LIMIT_SIZE = 10

  protected def hashKey(key: String): String = s"$DEFAULT_PREFIX_ONEM2MID_KEY$key"

  protected def listKey(key: String): String = s"$DEFAULT_PREFIX_ONEM2MID_KEY$DEFAULT_PREFIX_LIST_KEY$key"

  def apply():MongisCacheDataManager = new MongisCacheDataManager()
}

class MongisCacheDataManager extends MongoDataManager {

  private final val log = LoggerFactory.getLogger(this.getClass.getName)

  private val dar: RedisCacheDataManager = RedisCacheDataManager()

  override def init(): Unit = {
    super.init()
    dar.init()

    resourceCollection
      .watch()
      .fullDocument(FullDocument.UPDATE_LOOKUP)
      .subscribe(observer)

    log.info("Change Stream now listening ...")
  }


  override def countDocuments(ty: Int, pi: String): Future[Long] = {
    //val startTime = new Date().getTime
    //log.info(s"*** Redis cache Start : $startTime ***")

    val rcnt = dar.hget(hashKey(pi), s"_$ty")
    if (rcnt.isDefined) {
      log.debug(s"[ $pi ] countDocuments cache exists.")
      //log.info(s"*** Redis cache Finish : ${new Date().getTime - startTime} ***")
      Future(rcnt.get.toString.toLong)
    }
    else {
      log.debug(s"[ $pi ] countDocuments cache NOT exists.")
      _countDocuments(ty, pi)
    }
  }

  private def _countDocuments(ty: Int, pi: String): Future[Long] = {
    //val startTime = new Date().getTime
    //log.info(s"*** MongoDB countDocuments Start : $startTime ***")

    resourceCollection
      .countDocuments(Filters.and( Filters.eq("ty", ty), Filters.eq("pi", pi) ))
      .toFuture()
      .map(res => {
        //log.info(s"*** MongoDB countDocuments Finish : ${new Date().getTime - startTime} ***")
        // 캐시 설정 및 TTL 설정
        dar.hset(hashKey(pi), s"_$ty", res.intValue())
        res
      })
  }

  def discoveryDashboard(tg: String, ty: Int, lim: Int = DEFAULT_DASHBOARD_LIMIT_SIZE): Future[Seq[Document]] =
    find(tg)
      .flatMap(rec => {
        if (rec.isDefined) {
          log.info(s"rec : ${rec.get.toJson()}")
          val pi = rec.get.getString("ri")
          val rlst = dar.get[List[String]](listKey(pi))
          if (rlst.isDefined && rlst.get.nonEmpty) {
            log.debug("Discovery cache exists.")
            dar.setTTL(listKey(pi))
            Future(rlst.get.map(s => Document(s)))
          } else {
            log.debug("Discovery cache NOT exists.")
            _discoveryDashboard(rec, ty, lim)
          }
        }
        else throw new Exception(s"Discovery failed.")
      })

  private def _discoveryDashboard(rec: Option[Document], ty: Int, lim: Int): Future[Seq[Document]] =
    resourceCollection
      .find(Filters.and(Filters.eq("ty", ty), Filters.eq("pi", rec.get.getString("ri"))))
      .sort(Sorts.descending("ct"))
      .limit(lim)
      .toFuture()
      .map(res => {
        val pi = rec.get.getString("ri")
        dar.rpushAllWithTrim[String](listKey(pi), res.map(r => r.toJson()).toList, lim)
        dar.setTTL(listKey(pi))
        res
      })

  private val observer: Observer[ChangeStreamDocument[Document]] = new Observer[ChangeStreamDocument[Document]] {
    private var prevObjectId: String = ""

    private def getBsonValue(value: BsonValue): Any = {
      value match {
        case oid: BsonObjectId => oid.getValue
        case int32: BsonInt32 => int32.getValue
        case int64: BsonInt64 => int64.getValue
        case str: BsonString => str.getValue
        case dt: BsonDateTime => dt.getValue
        case d => d.toString
      }
    }
    private def getBsonValue(value: Option[BsonValue]): Any = {
      if (value.isDefined) getBsonValue(value.get)
      else ""
    }

    override def onNext(result: ChangeStreamDocument[Document]): Unit = {
      //log.info(s"Operation : ${result.getOperationType.getValue}")
      result.getOperationType match {
        case OperationType.INSERT =>
/*          log.info( s"ObjectID     : ${getBsonValue(result.getFullDocument.get("_id"))}\n" +
                    s"ResourceType : ${tys}\n" +
                    s"ResourceID   : ${getBsonValue(result.getFullDocument.get("ri"))}\n" +
                    s"ParentID     : ${pis}\n" )*/
          val oid = result.getDocumentKey.getString("_id").getValue
          if (oid.equals(prevObjectId)) {
            log.debug(s"중복 : ${oid}")
          }
          else {
            val pi = getBsonValue(result.getFullDocument.get("pi")).toString
            if (dar.exists(hashKey(pi))) {
              dar.hincrby(hashKey(pi), s"_${getBsonValue(result.getFullDocument.get("ty")).toString}", 1)
            }
            if (dar.exists(listKey(pi))) {
              log.debug("Cache exists - add new document")
              dar.lpushWithTrim[String](listKey(pi), result.getFullDocument.toJson(), DEFAULT_DASHBOARD_LIMIT_SIZE)
            }
            else log.debug("Cache Not exists - not to do")
          }
        case OperationType.UPDATE =>
          //log.info(s"FullDocument : ${result.getFullDocument}")
        case OperationType.DELETE =>
          val oid = result.getDocumentKey.getString("_id").getValue
          if (oid.equals(prevObjectId)) {
            log.debug(s"중복 : ${oid}")
          }
          else {
            val splt = oid.split("-")
            //log.info(s"pi : ${splt(2)}, ty : ${splt(0).substring(1).toInt.toString}")
            if (dar.exists(hashKey(splt(2)))) {
              log.debug(s"field : ${splt(0).substring(1).toInt.toString}")
              dar.hincrby(hashKey(splt(2)), s"_${splt(0).substring(1).toInt.toString}", -1)
            }
            dar.del(listKey(splt(2)))
            prevObjectId = oid
          }
        case _ => log.info(s"Untreated operation")
      }

    }

    override def onError(e: Throwable): Unit = {
      log.warn(s"observer error  : ${e.getMessage}")
    }

    override def onComplete(): Unit = {
      log.warn(s"observer complete")
    }
  }

}
