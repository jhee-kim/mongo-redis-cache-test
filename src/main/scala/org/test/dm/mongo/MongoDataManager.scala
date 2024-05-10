package org.test.dm.mongo

import com.mongodb.ConnectionString
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.{Filters, Projections, Sorts}
import org.mongodb.scala.{Document, MongoClient, MongoClientSettings, MongoCollection, MongoDatabase}
import org.slf4j.LoggerFactory
import MongoDataManager.{DB_NAME, DB_URL, RESOURCE_COLLECTION_NAME}

import java.util.Date
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object MongoDataManager {
  private final val DB_URL = "mongodb://10.10.30.116:27017"

  private final val DB_NAME = "mydb"
  private final val RESOURCE_COLLECTION_NAME = "resource"

  def apply() = new MongoDataManager()
}

class MongoDataManager {
  private final val log = LoggerFactory.getLogger(this.getClass.getName)

  private var _client: Option[MongoClient] = None

  def client: MongoClient = _client.getOrElse(throw new Exception("MongoDB initialize failed."))

  def db: MongoDatabase = client.getDatabase(DB_NAME)

  def resourceCollection: MongoCollection[Document] = db.getCollection(RESOURCE_COLLECTION_NAME)

  def init(): Unit = {
    val settings: MongoClientSettings = MongoClientSettings.builder()
      .applyConnectionString(new ConnectionString(DB_URL))
      .build()

    _client = Option(MongoClient(settings))
  }

  def release(): Unit =
    _client match {
      case Some(client) => client.close(); _client = None
      case None => // not to do
    }

  def countDocuments(ty: Int, pi: String): Future[Long] = {

    resourceCollection
      .countDocuments(Filters.and( Filters.eq("ty", ty), Filters.eq("pi", pi) ))
      .toFuture()
      .map(res => {
        res
      })
  }

  def find(srn: String): Future[Option[Document]] =
    resourceCollection
      .find(Filters.eq( "_srn", srn))
      .projection( Projections.excludeId() )
      .headOption()

  def discovery(tg: String, ty: Int, ofst: Int, lim: Int, sort: String): Future[(Int, Seq[Document])] =
    find(tg)
      .flatMap(rec => {
        if (rec.isDefined) {
          val pi = rec.get.getString("ri")
          countDocuments(ty, pi)
            .flatMap(cnt => {
              resourceCollection
                .find(Filters.and( Filters.eq("ty", ty), Filters.eq("pi", pi)))
                .sort(sortConvert(sort))
                .limit(Math.min(lim, 250))
                .skip(ofst)
                .toFuture()
                .map(docs =>(cnt.toInt, docs))
            })
        }
        else throw new Exception(s"Discovery failed.")
      })

  def discoveryDashboard(tg: String, ty: Int, ofst: Int, lim: Int, sort: String): Future[(Int, Seq[Document])] =
    find(tg)
      .flatMap(rec => {
        if (rec.isDefined) {
          val pi = rec.get.getString("ri")
          resourceCollection
            .find(Filters.and(Filters.eq("ty", ty), Filters.eq("pi", pi)))
            .sort(sortConvert(sort))
            .limit(Math.min(lim, 250))
            .skip(ofst)
            .toFuture()
            .map(docs => (0, docs))
        }
        else throw new Exception(s"DiscoveryDashboard failed.")
      })

  protected def sortConvert(str: String): Bson = {
    val splt = str.split(',')
    val sortType = if (splt.length > 1) splt(1) else "desc"
    sortType match {
      case "asc" => Sorts.ascending(splt(0))
      case _ => Sorts.descending(splt(0))
    }
  }
}
