package io.renku.jsonld

import java.io.Serializable

import cats.data.NonEmptyList
import io.circe.Json

abstract class JsonLD extends Product with Serializable {
  def toJson: Json
}

object JsonLD {

  def fromString(value: String): JsonLD = StringJsonLD(value)

  def entity(id: EntityId, types: NonEmptyList[EntityType], properties: NonEmptyList[(Property, JsonLD)]): JsonLD = ???

  private[jsonld] final case class StringJsonLD(value: String) extends JsonLD {
    override lazy val toJson = Json.fromString(value)
  }
}
