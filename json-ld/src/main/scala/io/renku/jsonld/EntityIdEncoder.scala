package io.renku.jsonld

/**
  * A type class that provides a conversion from a value of type `A` to a [[EntityId]] value.
  */
trait EntityIdEncoder[A] {
  def apply(a: A): EntityId
}

object EntityIdEncoder {

  final def instance[A](f: A => EntityId): EntityIdEncoder[A] = (a: A) => f(a)
}
