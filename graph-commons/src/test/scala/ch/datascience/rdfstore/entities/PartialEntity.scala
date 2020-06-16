package ch.datascience.rdfstore.entities

import io.renku.jsonld.{EntityId, EntityTypes, JsonLD, Property}

case class PartialEntity(maybeId: Option[EntityId], types: EntityTypes, properties: List[(Property, JsonLD)])

object PartialEntity {

  implicit class PartialJsonLDOps(partialEntity: PartialEntity) {
    def combine(other: PartialEntity): PartialEntity =
      other.copy(
        maybeId    = other.maybeId orElse partialEntity.maybeId,
        types      = EntityTypes(partialEntity.types.list ::: other.types.list),
        properties = partialEntity.properties ++ other.properties
      )

    def getOrElse[B >: JsonLD](default: => B): B =
      partialEntity.maybeId
        .map { entityId =>
          JsonLD.entity(entityId, partialEntity.types, partialEntity.properties)
        }
        .getOrElse(default)
  }
}
