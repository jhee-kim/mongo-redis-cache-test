package org.test.dm.mongis

import com.mongodb.client.model.changestream.OperationType
import org.bson._
import org.mongodb.scala.Observer
import org.mongodb.scala.bson.{BsonInt32, BsonObjectId, BsonValue, ObjectId}
import org.mongodb.scala.model.{Filters, Sorts}
import org.mongodb.scala.model.changestream.{ChangeStreamDocument, FullDocument}
import org.slf4j.LoggerFactory
import MongisDataManager.{DEFAULT_DASHBOARD_LIMIT_SIZE, hashKey, listKey}
import org.mongodb.scala.Document
import org.test.dm.mongo.MongoDataManager
import org.test.dm.redis.RedisDataManager

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object MongisDataManager {
  private final val DEFAULT_PREFIX_ONEM2MID_KEY = "_oneM2M."
  private final val DEFAULT_PREFIX_LIST_KEY = "li."

  private final val DEFAULT_DASHBOARD_LIMIT_SIZE = 10

  protected def hashKey(key: String): String = s"$DEFAULT_PREFIX_ONEM2MID_KEY$key"

  protected def listKey(key: String): String = s"$DEFAULT_PREFIX_ONEM2MID_KEY$DEFAULT_PREFIX_LIST_KEY$key"

  def apply():MongisDataManager = new MongisDataManager()
}

class MongisDataManager extends MongoDataManager {

  private final val log = LoggerFactory.getLogger(this.getClass.getName)

  private val dar: RedisDataManager = RedisDataManager()

  override def init(): Unit = {
    super.init()
    dar.init()

    dar.initTTL(dar.keys(hashKey("*")))

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
      dar.setTTL(hashKey(pi))
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
        dar.setTTL(hashKey(pi))
        res
      })
  }

  def discoveryDashboard(tg: String, ty: Int, lim: Int): Future[Seq[Document]] =
    find(tg)
      .flatMap(rec => {
        if (rec.isDefined) {
          val pi = rec.get.getString("ri")
          val rlst = dar.lgetAll(listKey(rec.get.getString("ri")))
          if (rlst.isDefined) {
            if (rlst.get.nonEmpty) {
              log.debug("Discovery cache exists.")
              dar.setTTL(listKey(pi))
              Future(rlst.get.map(s => new Document(s)))
            }
            else {
              log.debug("Discovery cache NOT exists.")
              _discoveryDashboard(rec, ty, lim)
            }
          }
          else {
            throw new Exception(s"Redis get failed.")
          }
        }
        else throw new Exception(s"Discovery failed.")
      })

  private def _discoveryDashboard(rec: Option[Document], ty: Int, lim: Int): Future[Seq[Document]] =
    resourceCollection
      .find(Filters.and(Filters.eq("ty", ty), Filters.eq("pi", rec.get.getString("ri"))))
      .sort(Sorts.descending("ct"))
      .limit(10)
      .toFuture()
      .map(res => {
        val pi = rec.get.getString("ri")
        dar.rpushAll(listKey(pi), res.map(r => r.toJson()))
        dar.trim(listKey(pi), 0, DEFAULT_DASHBOARD_LIMIT_SIZE - 1)
        dar.setTTL(listKey(pi))
        res
      })

  private val observer: Observer[ChangeStreamDocument[Document]] = new Observer[ChangeStreamDocument[Document]] {
    private var prevObjectId: ObjectId = _

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
          val pi = getBsonValue(result.getFullDocument.get("pi")).toString
          if (dar.exists(hashKey(pi))) {
            dar.hincrby(hashKey(pi), s"_${getBsonValue(result.getFullDocument.get("ty")).toString}", 1)
          }
          if (dar.exists(listKey(pi))) {
            log.debug("Cache exists - add new document")
            dar.lpush(listKey(pi), result.getFullDocument.toJson())
            dar.trim(listKey(pi), 0, DEFAULT_DASHBOARD_LIMIT_SIZE - 1)
          }
          else log.debug("Cache Not exists - not to do")
        case OperationType.UPDATE =>
          //log.info(s"FullDocument : ${result.getFullDocument}")
        case OperationType.DELETE =>
          val oid = result.getDocumentKey.getObjectId("_id").getValue
          if (oid.equals(prevObjectId)) {
            log.debug(s"중복 : ${oid}")
          }
          else {
            /*val splt = oid.split("-")
            //log.info(s"pi : ${splt(2)}, ty : ${splt(0).substring(1).toInt.toString}")
            if (dar.exists(hashKey(splt(2)))) {
              dar.hincrby(hashKey(splt(2)), s"_${splt(0).substring(1).toInt.toString}", -1)
            }
            dar.del(listKey(splt(2)))
            prevObjectId = oid*/
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
