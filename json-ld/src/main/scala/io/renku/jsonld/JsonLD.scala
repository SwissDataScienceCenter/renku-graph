package io.renku.jsonld

import java.io.Serializable

import cats.data.NonEmptyList
import io.circe.{Encoder, Json}

abstract class JsonLD extends Product with Serializable {
  def toJson: Json
}

object JsonLD {
  import io.circe.syntax._

  def entity(id: EntityId, types: NonEmptyList[EntityType], properties: NonEmptyList[(Property, JsonLD)]): JsonLD =
    JsonLDEntity(id, types, properties)

  def fromString(value: String):   JsonLD = JsonLDValue(value)
  def fromInt(value:    Int):      JsonLD = JsonLDValue(value)
  def fromLong(value:   Long):     JsonLD = JsonLDValue(value)
  def fromEntityId(id:  EntityId): JsonLD = JsonLDEntityId(id)

  private[jsonld] final case class JsonLDEntity(id:         EntityId,
                                                types:      NonEmptyList[EntityType],
                                                properties: NonEmptyList[(Property, JsonLD)])
      extends JsonLD {

    override lazy val toJson: Json = Json.obj(
      List(
        "@id"   -> id.asJson,
        "@type" -> Json.arr(types.toList.map(_.asJson): _*)
      ) ++ properties.map { case (property, value) => property.url -> Json.arr(value.toJson) }.toList: _*
    )
  }

  private[jsonld] final case class JsonLDValue[V](value: V)(implicit encoder: Encoder[V]) extends JsonLD {
    override lazy val toJson = Json.obj("@value" -> value.asJson)
  }
  private[jsonld] final case class JsonLDEntityId[V <: EntityId](id: V)(implicit encoder: Encoder[V]) extends JsonLD {
    override lazy val toJson = Json.obj("@id" -> id.asJson)
  }
}
