package io.renku.jsonld

/**
  * A type class that provides a conversion from a value of type `A` to a [[JsonLD]] value.
  */
trait JsonLDEncoder[A] extends Serializable {

  def apply(a: A): JsonLD
}

object JsonLDEncoder {

  final def instance[A](f: A => JsonLD): JsonLDEncoder[A] = new JsonLDEncoder[A] {
    final def apply(a: A): JsonLD = f(a)
  }

  final def entityId[A](f: A => EntityId): JsonLDEncoder[A] = new JsonLDEncoder[A] {
    final def apply(a: A): JsonLD = JsonLD.fromEntityId(f(a))
  }

  final implicit val encodeString: JsonLDEncoder[String] = new JsonLDEncoder[String] {
    final def apply(a: String): JsonLD = JsonLD.fromString(a)
  }

  final implicit val encodeInt: JsonLDEncoder[Int] = new JsonLDEncoder[Int] {
    final def apply(a: Int): JsonLD = JsonLD.fromInt(a)
  }

  final implicit val encodeLong: JsonLDEncoder[Long] = new JsonLDEncoder[Long] {
    final def apply(a: Long): JsonLD = JsonLD.fromLong(a)
  }
}
