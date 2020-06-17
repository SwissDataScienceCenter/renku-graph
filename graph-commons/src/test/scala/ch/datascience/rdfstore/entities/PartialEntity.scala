/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

  def apply(id: EntityId): PartialEntity = new PartialEntity(Some(id), maybeTypes = None, Nil)

  def apply(first: (Property, JsonLD), other: (Property, JsonLD)*): PartialEntity =
    new PartialEntity(maybeId = None, maybeTypes = None, (first +: other).toList)

  implicit val semigroup: Semigroup[PartialEntity] = (x: PartialEntity, y: PartialEntity) =>
    x.copy(
      maybeId    = y.maybeId orElse x.maybeId,
      maybeTypes = y.maybeTypes.map(_.list) |+| x.maybeTypes.map(_.list) map EntityTypes.apply,
      properties = y.properties.foldLeft(x.properties) {
        case (originalList, (property, value)) =>
          val index = originalList.indexWhere(_._1 == property)

          if (index > -1) originalList.updated(index, property -> value)
          else originalList :+ (property -> value)
      }
    )

  implicit class PartialEntityOps(partialEntity: Either[Exception, PartialEntity]) {

    lazy val getOrFail: JsonLD = partialEntity flatMap toJsonLD fold (throw _, identity)

    private def toJsonLD(partialEntity: PartialEntity): Either[Exception, JsonLD] =
      for {
        entityId   <- partialEntity.maybeId.toEither(s"No entityId in $partialEntity")
        types      <- partialEntity.maybeTypes.toEither(s"No entityTypes in $partialEntity")
        properties <- NonEmptyList.fromList(partialEntity.properties).toEither(s"No properties in $partialEntity")
      } yield JsonLD.entity(entityId, types, properties, Nil: _*)

    private implicit class OptionOps[T](maybeValue: Option[T]) {
      def toEither(errorMessage: String): Either[Exception, T] = Either.fromOption(
        maybeValue,
        ifNone = new Exception(errorMessage)
      )
    }
  }
}
