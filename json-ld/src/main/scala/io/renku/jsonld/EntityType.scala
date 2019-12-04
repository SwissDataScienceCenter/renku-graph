package io.renku.jsonld

abstract class EntityType extends Product with Serializable

object EntityType {

  def fromUrl(url: String): EntityType = UrlEntityType(url)

  private[jsonld] final case class UrlEntityType(value: String) extends EntityType
}
