/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.renku.jsonld.flatten

import cats.syntax.all._
import io.renku.jsonld.JsonLD._
import io.renku.jsonld.{EntityId, JsonLD, Property, Reverse}

import scala.annotation.tailrec

private[jsonld] trait Flatten extends IDValidation {

  @tailrec
  protected[flatten] final def deNest(toProcess:        List[JsonLD],
                                      topLevelEntities: List[JsonLD]
  ): Either[MalformedJsonLD, List[JsonLD]] = toProcess match {
    case (entity: JsonLDEntity) :: leftToProcess =>
      val processNext =
        extractEntityProperties(entity.properties) ++ extractReverseProperties(entity.reverse.properties)
      val (currentEntityDeNested, edges) = transformEntityProperties(entity)
      deNest(processNext ::: leftToProcess, topLevelEntities ::: currentEntityDeNested :: edges)
    case JsonLDArray(jsons) :: leftToProcess =>
      deNest(jsons.toList ::: leftToProcess, topLevelEntities)
    case _ :: leftToProcess =>
      deNest(leftToProcess, topLevelEntities)
    case Nil =>
      topLevelEntities.asRight
  }

  private def extractEntityProperties(properties: Map[Property, JsonLD]) =
    properties.foldLeft(List.empty[JsonLDEntity]) {
      case (acc, (_, nestedEntity: JsonLDEntity)) => nestedEntity +: acc
      case (acc, (_, array: JsonLDArray)) =>
        array.jsons.collect { case entity: JsonLDEntity => entity }.toList ++ acc
      case (acc, _) => acc
    }

  private def extractReverseProperties(properties: Map[Property, JsonLD]): List[JsonLDEntity] =
    properties.foldLeft(List.empty[JsonLDEntity]) {
      case (acc, (_, nestedEntity: JsonLDEntity)) => nestedEntity +: acc
      case (acc, (_, array: JsonLDArray)) =>
        array.jsons.collect { case entity: JsonLDEntity => entity }.toList ++ acc
      case (acc, (_, _)) => acc
    }

  private def transformEntityProperties(entity: JsonLDEntity): (JsonLDEntity, List[JsonLDEdge]) = {

    val flattenedEntity = entity.copy(
      properties = cleanProperties(entity.properties),
      reverse = Reverse.empty
    )

    val edges = findReverseProperties(entity.reverse).flatMap { case (property, sources) =>
      sources.map(JsonLD.edge(_, property, entity.id))
    }.toList

    flattenedEntity -> edges
  }

  private def cleanProperties(properties: Map[Property, JsonLD]): Map[Property, JsonLD] = properties.map {
    case (property, JsonLDEntity(id, _, _, _)) => (property, JsonLDEntityId(id))
    case (property, JsonLDArray(jsons)) =>
      val cleanedJsons = jsons.map {
        case JsonLDEntity(id, _, _, _) => JsonLDEntityId(id)
        case other                     => other
      }
      (property, JsonLD.arr(cleanedJsons: _*))
    case other => other
  }

  private def findReverseProperties(reverse: Reverse): Map[Property, List[EntityId]] =
    reverse.properties.foldLeft(Map.empty[Property, List[EntityId]]) {
      case (properties, (property, JsonLDEntity(id, _, _, _))) => properties + (property -> List(id))
      case (properties, (property, JsonLDEntityId(id)))        => properties + (property -> List(id))
      case (properties, (property, JsonLDArray(jsons))) =>
        properties +
          (property -> jsons.foldLeft(List.empty[EntityId]) {
            case (entityIds, JsonLDEntity(id, _, _, _)) => entityIds ::: id :: Nil
            case (entityIds, JsonLDEntityId(id))        => entityIds ::: id :: Nil
            case (entityIds, _)                         => entityIds
          })
      case (properties, _) => properties
    }
}
