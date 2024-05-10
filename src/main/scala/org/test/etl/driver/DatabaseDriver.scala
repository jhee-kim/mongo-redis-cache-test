package org.test.etl.driver

import org.test.etl.models.BaseModel

trait DatabaseDriver {

  def init(): Unit

  def release(): Unit

 // def c[M <: BaseModel](model: M): M

}
