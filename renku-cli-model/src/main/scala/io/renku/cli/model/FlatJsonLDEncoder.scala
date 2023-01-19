package io.renku.cli.model

import io.renku.jsonld.JsonLD.{JsonLDArray, MalformedJsonLD}
import io.renku.jsonld.{JsonLD, JsonLDEncoder}

trait FlatJsonLDEncoder[A] extends JsonLDEncoder[A] {
  override def apply(value: A): JsonLDArray
}

object FlatJsonLDEncoder {
  def unsafe[A](encode: A => JsonLD): FlatJsonLDEncoder[A] =
    value =>
      encode(value).flatten
        .flatMap(_.asArray.map(JsonLDArray).toRight(MalformedJsonLD(s"JsonLD is not an array!")))
        .fold(throw _, identity)
}
