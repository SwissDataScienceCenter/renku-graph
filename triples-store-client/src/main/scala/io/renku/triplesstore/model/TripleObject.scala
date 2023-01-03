package io.renku.triplesstore.model

import io.renku.jsonld.EntityId

sealed trait TripleObject extends Any {
  type T
  def value: T
}

object TripleObject {

  final case class Boolean(value: scala.Boolean) extends AnyVal with TripleObject {
    override type T = scala.Boolean
  }

  final case class Int(value: scala.Int) extends AnyVal with TripleObject {
    override type T = scala.Int
  }

  final case class Long(value: scala.Long) extends AnyVal with TripleObject {
    override type T = scala.Long
  }

  final case class Float(value: scala.Float) extends AnyVal with TripleObject {
    override type T = scala.Float
  }

  final case class Double(value: scala.Double) extends AnyVal with TripleObject {
    override type T = scala.Double
  }

  final case class String(value: Predef.String) extends AnyVal with TripleObject {
    override type T = Predef.String
  }

  final case class Iri(value: EntityId) extends TripleObject {
    override type T = EntityId
  }
}
