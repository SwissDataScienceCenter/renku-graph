package io.renku.jsonld

import io.circe.{Encoder, Json}

abstract class EntityId(val value: String) extends Product with Serializable {
  override lazy val toString: String = value
}

object EntityId {

  def fromAbsoluteUri(uri: String): EntityId = AbsoluteUriEntityId(uri)
  def fromRelativeUri(uri: String): EntityId = RelativeUriEntityId(uri)

  private[jsonld] final case class AbsoluteUriEntityId(override val value: String) extends EntityId(value)
  private[jsonld] final case class RelativeUriEntityId(override val value: String) extends EntityId(value)

  implicit val entityIdJsonEncoder: Encoder[EntityId] = Encoder.instance(id => Json.fromString(id.value))
}
