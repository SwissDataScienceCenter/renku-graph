package ch.datascience.rdfstore.entities

import cats.data.NonEmptyList
import cats.implicits._
import cats.kernel.Semigroup
import io.renku.jsonld.{EntityId, EntityTypes, JsonLD, Property}

final case class PartialEntity(maybeId:    Option[EntityId],
                               maybeTypes: Option[EntityTypes],
                               properties: List[(Property, JsonLD)])

object PartialEntity {

  def apply(maybeId: Option[EntityId],
            types:   EntityTypes,
            first:   (Property, JsonLD),
            other:   (Property, JsonLD)*): PartialEntity =
    PartialEntity(
      maybeId,
      types.some,
      (first +: other).toList
    )

  def apply(id: EntityId, types: EntityTypes, first: (Property, JsonLD), other: (Property, JsonLD)*): PartialEntity =
    PartialEntity(
      id.some,
      types.some,
      (first +: other).toList
    )

  def apply(id: EntityId, first: (Property, JsonLD), other: (Property, JsonLD)*): PartialEntity =
    PartialEntity(maybeId = Some(id), maybeTypes = None, (first +: other).toList)

  def apply(types: EntityTypes, first: (Property, JsonLD), other: (Property, JsonLD)*): PartialEntity =
    PartialEntity(
      None,
      types.some,
      (first +: other).toList
    )

  def apply(types: EntityTypes): PartialEntity = new PartialEntity(maybeId = None, Some(types), Nil)

  implicit val semigroup: Semigroup[PartialEntity] = new Semigroup[PartialEntity] {
    override def combine(x: PartialEntity, y: PartialEntity): PartialEntity = x.copy(
      maybeId = x.maybeId orElse y.maybeId,
      maybeTypes = (y.maybeTypes -> x.maybeTypes) match {
        case (None, None)            => None
        case (types @ Some(_), None) => types
        case (None, types @ Some(_)) => types
        case types =>
          types.mapN { (xTypes, yTypes) =>
            EntityTypes(xTypes.list ::: yTypes.list)
          }
      },
      properties = y.properties ++ x.properties
    )
  }

  implicit class PartialEntityOps(partialEntity: Either[Exception, PartialEntity]) {
    lazy val getOrFail: JsonLD =
      partialEntity flatMap toJsonLD fold (throw _, identity)

    private def toJsonLD(partialEntity: PartialEntity): Either[Exception, JsonLD] =
      for {
        entityId <- Either.fromOption(
                     partialEntity.maybeId,
                     new Exception(
                       s"No entity Id in partial entity ${partialEntity.maybeTypes} ${partialEntity.properties}"
                     )
                   )
        types <- Either.fromOption(
                  partialEntity.maybeTypes,
                  new Exception(
                    s"No entityTypes in partial entity ${partialEntity.maybeId} ${partialEntity.properties}"
                  )
                )
        properties <- Either.fromOption(
                       NonEmptyList.fromList(partialEntity.properties),
                       new Exception(
                         s"No properties in partial entity ${partialEntity.maybeId} ${partialEntity.maybeTypes}"
                       )
                     )
      } yield JsonLD.entity(entityId, types, properties, Nil: _*)

  }
}
