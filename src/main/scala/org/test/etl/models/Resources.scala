package org.test.etl.models

object Resources {

  sealed trait Resource

  case class CSE() extends Resource
  case class AE(ri: String, pi: String, ct: String, lt: String) extends Resource
  case class Container() extends Resource
  case class ContentInstance() extends Resource

}
