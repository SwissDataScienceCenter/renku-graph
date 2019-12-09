package io.renku.jsonld

import io.circe.{Encoder, Json}

abstract class EntityType(val value: String) extends Product with Serializable

object EntityType {

  def fromUrl(url:           String):   EntityType = UrlEntityType(url)
  def fromProperty(property: Property): EntityType = UrlEntityType(property.url)

  private[jsonld] final case class UrlEntityType(override val value: String) extends EntityType(value)

  implicit val entityTypeJsonEncoder: Encoder[EntityType] = Encoder.instance(t => Json.fromString(t.value))
}
