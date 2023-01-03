package io.renku.triplesstore.model

import cats.Contravariant
import cats.syntax.all._
import io.renku.jsonld.{EntityId, Property}

trait TripleObjectEncoder[O] extends (O => TripleObject)

object TripleObjectEncoder {

  def apply[V](implicit instance: TripleObjectEncoder[V]): TripleObjectEncoder[V] = instance

  def instance[T](f: T => TripleObject): TripleObjectEncoder[T] = (t: T) => f(t)

  implicit lazy val contravariant: Contravariant[TripleObjectEncoder] = new Contravariant[TripleObjectEncoder] {
    def contramap[A, B](fa: TripleObjectEncoder[A])(f: B => A): TripleObjectEncoder[B] =
      TripleObjectEncoder[B](b => fa(f(b)))
  }

  object implicits extends implicits
  trait implicits {
    implicit val booleanEncoder: TripleObjectEncoder[scala.Boolean] = TripleObjectEncoder.instance(TripleObject.Boolean)
    implicit val intEncoder:     TripleObjectEncoder[scala.Int]     = TripleObjectEncoder.instance(TripleObject.Int)
    implicit val longEncoder:    TripleObjectEncoder[scala.Long]    = TripleObjectEncoder.instance(TripleObject.Long)
    implicit val floatEncoder:   TripleObjectEncoder[scala.Float]   = TripleObjectEncoder.instance(TripleObject.Float)
    implicit val doubleEncoder:  TripleObjectEncoder[scala.Double]  = TripleObjectEncoder.instance(TripleObject.Double)
    implicit val stringEncoder:  TripleObjectEncoder[Predef.String] = TripleObjectEncoder.instance(TripleObject.String)
    implicit val entityIdEncoder: TripleObjectEncoder[EntityId] = TripleObjectEncoder.instance(TripleObject.Iri)
    implicit val propertyEncoder: TripleObjectEncoder[Property] = entityIdEncoder.contramap(p => EntityId.of(p))
  }
}
