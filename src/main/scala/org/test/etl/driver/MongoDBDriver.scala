package org.test.etl.driver

import com.mongodb.ConnectionString
import org.mongodb.scala.{Document, MongoClient, MongoClientSettings, MongoCollection, MongoDatabase}
import org.slf4j.LoggerFactory
import org.test.etl.driver.MongoDBDriver.{DB_NAME, DB_URL, RESOURCE_COLLECTION_NAME}
import org.test.etl.models.BaseModel

object MongoDBDriver {
  private final val DB_URL = "mongodb://localhost:37017"

  private final val DB_NAME = "mydb"
  private final val RESOURCE_COLLECTION_NAME = "resource"

  def apply() = new MongoDBDriver()
}

class MongoDBDriver extends DatabaseDriver {
  private final val log = LoggerFactory.getLogger(this.getClass.getName)

  private var _client: Option[MongoClient] = None

  def client: MongoClient = _client.getOrElse(throw new Exception("MongoDB initialize failed."))

  def db: MongoDatabase = client.getDatabase(DB_NAME)

  def resourceCollection: MongoCollection[Document] = db.getCollection(RESOURCE_COLLECTION_NAME)

  override def init(): Unit = {
    val settings: MongoClientSettings = MongoClientSettings.builder()
      .applyConnectionString(new ConnectionString(DB_URL))
      .build()

    _client = Option(MongoClient(settings))
  }

  override def release(): Unit =
    _client match {
      case Some(client) => client.close(); _client = None
      case None => // not to do
    }

}
