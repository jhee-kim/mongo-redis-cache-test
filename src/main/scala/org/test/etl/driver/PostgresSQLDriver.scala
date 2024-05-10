package org.test.etl.driver

import slick.jdbc.JdbcBackend.Database

class PostgresSQLDriver extends DatabaseDriver {

  val db = Database.forConfig("pgdb")

  override def init(): Unit = {

  }

  override def release(): Unit = {

  }

}
