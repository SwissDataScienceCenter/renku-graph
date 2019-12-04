package io.renku.jsonld

abstract class EntityId extends Product with Serializable

object EntityId {

  def fromAbsoluteUri(uri: String): EntityId = AbsoluteUriEntityId(uri)
  def fromRelativeUri(uri: String): EntityId = RelativeUriEntityId(uri)

  private[jsonld] final case class AbsoluteUriEntityId(value: String) extends EntityId
  private[jsonld] final case class RelativeUriEntityId(value: String) extends EntityId
}
