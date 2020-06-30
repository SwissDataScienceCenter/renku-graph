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
import io.renku.jsonld._

final case class PartialEntity(maybeId:      Option[EntityId],
                               maybeTypes:   Option[EntityTypes],
                               properties:   List[(Property, JsonLD)],
                               maybeReverse: Option[Reverse])

object PartialEntity {

  def apply(maybeId: Option[EntityId],
            types:   EntityTypes,
            first:   (Property, JsonLD),
            other:   (Property, JsonLD)*): PartialEntity =
    PartialEntity(
      maybeId,
      types.some,
      (first +: other).toList,
      maybeReverse = None
    )

  def apply(id: EntityId, types: EntityTypes, first: (Property, JsonLD), other: (Property, JsonLD)*): PartialEntity =
    PartialEntity(
      id.some,
      types.some,
      (first +: other).toList,
      maybeReverse = None
    )

  def apply(types: EntityTypes, first: (Property, JsonLD), other: (Property, JsonLD)*): PartialEntity =
    PartialEntity(
      maybeId      = None,
      maybeTypes   = types.some,
      properties   = (first +: other).toList,
      maybeReverse = None
    )

  def apply(types:   EntityTypes,
            reverse: Reverse,
            first:   (Property, JsonLD),
            other:   (Property, JsonLD)*): PartialEntity =
    PartialEntity(
      maybeId      = None,
      maybeTypes   = types.some,
      properties   = (first +: other).toList,
      maybeReverse = Some(reverse)
    )

  def apply(types: EntityTypes): PartialEntity =
    new PartialEntity(maybeId = None, Some(types), properties = Nil, maybeReverse = None)

  def apply(id: EntityId): PartialEntity =
    new PartialEntity(Some(id), maybeTypes = None, properties = Nil, maybeReverse = None)

  def apply(first: (Property, JsonLD), other: (Property, JsonLD)*): PartialEntity =
    new PartialEntity(maybeId = None, maybeTypes = None, (first +: other).toList, maybeReverse = None)

  implicit val semigroup: Semigroup[PartialEntity] = (x: PartialEntity, y: PartialEntity) =>
    x.copy(
      maybeId      = y.maybeId orElse x.maybeId,
      maybeTypes   = y.maybeTypes.map(_.list) |+| x.maybeTypes.map(_.list) map EntityTypes.apply,
      properties   = x.properties merge y.properties,
      maybeReverse = x.maybeReverse |+| y.maybeReverse
    )

  implicit class PartialEntityOps(partialEntity: Either[Exception, PartialEntity]) {

    lazy val getOrFail: JsonLD = partialEntity flatMap toJsonLD fold (throw _, identity)

    private def toJsonLD(partialEntity: PartialEntity): Either[Exception, JsonLD] =
      for {
        entityId   <- partialEntity.maybeId.toEither(s"No entityId in $partialEntity")
        types      <- partialEntity.maybeTypes.toEither(s"No entityTypes in $partialEntity")
        reverse    <- partialEntity.maybeReverse.getOrElse(Reverse.empty).asRight[Exception]
        properties <- NonEmptyList.fromList(partialEntity.properties).toEither(s"No properties in $partialEntity")
      } yield JsonLD.entity(entityId, types, reverse, properties)

    private implicit class OptionOps[T](maybeValue: Option[T]) {
      def toEither(errorMessage: String): Either[Exception, T] = Either.fromOption(
        maybeValue,
        ifNone = new Exception(errorMessage)
      )
    }
  }
}
