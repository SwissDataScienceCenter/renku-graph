package io.renku.triplesstore.model

trait QuadsEncoder[T] extends (T => List[Quad])

object QuadsEncoder {

  def instance[T](f: T => List[Quad]): QuadsEncoder[T] = (t: T) => f(t)
}
