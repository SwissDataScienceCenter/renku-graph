package io.renku.graph.model.entities

import io.circe.DecodingFailure
import io.renku.jsonld.JsonLDDecoder

object IntermediateShim {

  def failingDecoder[A](msg: String = "not implemented"): JsonLDDecoder[A] =
    _ => Left(DecodingFailure(msg, Nil))

}
