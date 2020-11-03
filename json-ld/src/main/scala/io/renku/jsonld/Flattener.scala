package io.renku.jsonld

import cats.data.NonEmptyList
import io.renku.jsonld.JsonLD.{JsonLDArray, JsonLDEntity, JsonLDEntityId, JsonLDNull, JsonLDValue, MalformedJsonLD}
import cats.syntax.all._
import scala.annotation.tailrec

private[jsonld] object Flattener {

  def flatten(jsonLD: JsonLD): Either[MalformedJsonLD, JsonLD] =
    jsonLD match {
      case array: JsonLDArray =>
        array.jsons
          .foldLeft(Either.right[MalformedJsonLD, List[JsonLD]](List.empty[JsonLD])) {
            case (acc, jsonLDEntity: JsonLDEntity) =>
              for {
                jsons    <- deNest(List(jsonLDEntity), List.empty[JsonLDEntity])
                accRight <- acc
              } yield (accRight ++ jsons)
            case (acc, other) => acc.map(other +: _)
          }
          .map(flattenedJsons => JsonLD.arr(flattenedJsons: _*))

      case entity:   JsonLDEntity      => deNest(List(entity), Nil).map(flattenedJsons => JsonLD.arr(flattenedJsons: _*))
      case entityId: JsonLDEntityId[_] => entityId.asRight
      case value:    JsonLDValue[_]    => value.asRight
      case JsonLDNull => JsonLDNull.asRight
    }

  @tailrec
  private def deNest(toProcess: List[JsonLD], topLevelEntities: List[JsonLD]): Either[MalformedJsonLD, List[JsonLD]] =
    toProcess match {
      case (x: JsonLDEntity) :: xs =>
        val processNext: List[JsonLDEntity] = x.properties.foldLeft(List.empty[JsonLDEntity]) {
          case (acc, (_, entity: JsonLDEntity)) => entity +: acc
          case (acc, (_, array: JsonLDArray)) =>
            array.jsons.collect { case (entity: JsonLDEntity) => entity }.toList ++ acc
          case (acc, _) => acc
        }
        val currentEntityDeNested: JsonLDEntity = transformEntityProperties(x)
        deNest(processNext ++ xs, currentEntityDeNested +: topLevelEntities)
      case _ :: xs => deNest(xs, topLevelEntities)
      case Nil     => topLevelEntities.asRight
    }

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

  private def deNestProperties(entity:                  JsonLDEntity,
                               properties:              List[(Property, JsonLD)],
                               replacePropertyFunction: (JsonLDEntity, (Property, JsonLD)) => JsonLDEntity,
                               topLevelEntities:        List[JsonLDEntity]
  ) = ???

  //    properties.foldLeft(entity, topLevelEntities) {
  //      case ((mainEntity, entities), (property, jsonLDEntity: JsonLDEntity)) =>
  //        val (updatedNestedEntity, updatedEntities) = deNest(jsonLDEntity, entities)
  //        replacePropertyFunction(mainEntity,
  //                                property -> JsonLDEntityId(updatedNestedEntity.id)
  //        ) -> (updatedEntities :+ updatedNestedEntity)
  //      case ((mainEntity, entities), (property, JsonLDArray(jsonLDs))) =>
  //        val (arrayElements, deNestedEntities) = ??? // TODO: fold this: deNest(jsonLDs, entities)
  //        replacePropertyFunction(mainEntity, property -> JsonLDArray(arrayElements)) -> deNestedEntities
  //      case ((mainEntity, entities), (_, _)) => mainEntity -> entities
  //    }

}
