package io.renku.jsonld

import cats.data.NonEmptyList
import io.renku.jsonld.JsonLD.{JsonLDArray, JsonLDEntity, JsonLDEntityId, JsonLDNull, JsonLDValue, MalformedJsonLD, arr}
import cats.syntax.all._

import scala.annotation.tailrec

private[jsonld] object Flattener {

  def flatten(jsonLD: JsonLD): Either[MalformedJsonLD, JsonLD] =
    jsonLD match {
      case array:    JsonLDArray       => flattenArray(array)
      case entity:   JsonLDEntity      => deNest(List(entity), Nil).flatMap(checkForUniqueIds)
      case entityId: JsonLDEntityId[_] => entityId.asRight
      case value:    JsonLDValue[_]    => value.asRight
      case JsonLDNull => JsonLDNull.asRight
    }

  private def flattenArray(array: JsonLDArray) =
    for {
      flattenedJsons <- array.jsons
                          .foldLeft(Either.right[MalformedJsonLD, List[JsonLD]](List.empty[JsonLD])) {
                            case (acc, jsonLDEntity: JsonLDEntity) =>
                              for {
                                jsons    <- deNest(List(jsonLDEntity), List.empty[JsonLDEntity])
                                accRight <- acc
                              } yield (accRight ++ jsons)
                            case (acc, other) => acc.map(other +: _)
                          }
      flattenedArray <- checkForUniqueIds(flattenedJsons.distinct)
    } yield flattenedArray

  @tailrec
  private def deNest(toProcess: List[JsonLD], topLevelEntities: List[JsonLD]): Either[MalformedJsonLD, List[JsonLD]] =
    toProcess match {
      case (entity: JsonLDEntity) :: entities =>
        val processNext: List[JsonLDEntity] =
          extractEntityProperties(entity.properties) ++ extractReverseProperties(entity.reverse.properties)
        val currentEntityDeNested: JsonLDEntity = transformEntityProperties(entity)
        deNest(processNext ++ entities, currentEntityDeNested +: topLevelEntities)
      case _ :: xs => deNest(xs, topLevelEntities)
      case Nil     => topLevelEntities.asRight
    }

  private def extractEntityProperties(properties: NonEmptyList[(Property, JsonLD)]) =
    properties.foldLeft(List.empty[JsonLDEntity]) {
      case (acc, (_, nestedEntity: JsonLDEntity)) => nestedEntity +: acc
      case (acc, (_, array: JsonLDArray)) =>
        array.jsons.collect { case (entity: JsonLDEntity) => entity }.toList ++ acc
      case (acc, _) => acc
    }

  private def extractReverseProperties(properties: List[(Property, JsonLD)]): List[JsonLDEntity] =
    properties.collect { case (_, entities: JsonLDArray) =>
      entities.asArray.toList.flatten.collect { case (entity: JsonLDEntity) => entity }
    }.flatten

  private def transformEntityProperties(entity: JsonLDEntity): JsonLDEntity = {
    val cleanedProperties        = cleanProperties(entity.properties.toList)
    val cleanedReverseProperties = cleanProperties(entity.reverse.properties)
    entity.copy(properties = NonEmptyList.fromListUnsafe(cleanedProperties),
                reverse = Reverse.fromListUnsafe(cleanedReverseProperties)
    )
  }

  private def cleanProperties(properties: List[(Property, JsonLD)]): List[(Property, JsonLD)] = properties.map {
    case (property, JsonLDEntity(id, _, _, _)) => (property, JsonLDEntityId(id))
    case (property, JsonLDArray(jsons)) =>
      val cleanedJsons = jsons.map {
        case JsonLDEntity(id, _, _, _) => JsonLDEntityId(id)
        case other                     => other
      }
      (property, JsonLD.arr(cleanedJsons: _*))
    case other => other
  }

  private def checkForUniqueIds(flattenedJsons: List[JsonLD]) = if (areIdsUnique(flattenedJsons))
    Right(JsonLD.arr(flattenedJsons: _*))
  else
    Left(MalformedJsonLD("Some entities share an ID even though they're not the same"))

  private def areIdsUnique(jsons: List[JsonLD]): Boolean =
    jsons
      .collect { case entity: JsonLDEntity => entity }
      .groupBy(entity => entity.id)
      .forall { case (_, entitiesPerId) =>
        entitiesPerId.forall(_ == entitiesPerId.head)
      }
}
