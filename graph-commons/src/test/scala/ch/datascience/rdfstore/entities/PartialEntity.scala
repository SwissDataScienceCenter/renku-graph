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

package ch.datascience.rdfstore.entities

import cats.kernel.Semigroup
import cats.syntax.all._
import io.renku.jsonld._

final case class PartialEntity(maybeId:      Option[EntityId],
                               maybeTypes:   Option[EntityTypes],
                               properties:   Map[Property, JsonLD],
                               maybeReverse: Option[Reverse]
)

object PartialEntity {

  def apply(id: EntityId, types: EntityTypes, first: (Property, JsonLD), other: (Property, JsonLD)*): PartialEntity =
    PartialEntity(
      id.some,
      types.some,
      other.toMap + first,
      maybeReverse = None
    )

  def apply(types: EntityTypes, first: (Property, JsonLD), other: (Property, JsonLD)*): PartialEntity =
    PartialEntity(
      maybeId = None,
      maybeTypes = types.some,
      properties = other.toMap + first,
      maybeReverse = None
    )

  def apply(types:   EntityTypes,
            reverse: Reverse,
            first:   (Property, JsonLD),
            other:   (Property, JsonLD)*
  ): PartialEntity =
    PartialEntity(
      maybeId = None,
      maybeTypes = types.some,
      properties = other.toMap + first,
      maybeReverse = Some(reverse)
    )

  def apply(types: EntityTypes): PartialEntity =
    new PartialEntity(maybeId = None, Some(types), properties = Map.empty, maybeReverse = None)

  def apply(first: (Property, JsonLD), other: (Property, JsonLD)*): PartialEntity =
    new PartialEntity(maybeId = None, maybeTypes = None, properties = other.toMap + first, maybeReverse = None)

  implicit val semigroup: Semigroup[PartialEntity] = (x: PartialEntity, y: PartialEntity) =>
    x.copy(
      maybeId = y.maybeId orElse x.maybeId,
      maybeTypes = y.maybeTypes.map(_.list) |+| x.maybeTypes.map(_.list) map EntityTypes.apply,
      properties = x.properties ++ y.properties,
      maybeReverse = x.maybeReverse |+| y.maybeReverse
    )

  implicit class PartialEntityOps(partialEntity: Either[Exception, PartialEntity]) {

    lazy val getOrFail: JsonLD = partialEntity.flatMap(toJsonLD).fold(throw _, identity)

    private def toJsonLD(partialEntity: PartialEntity): Either[Exception, JsonLD] =
      for {
        entityId   <- partialEntity.maybeId.toEither(s"No entityId in $partialEntity")
        types      <- partialEntity.maybeTypes.toEither(s"No entityTypes in $partialEntity")
        reverse    <- partialEntity.maybeReverse.getOrElse(Reverse.empty).asRight[Exception]
        properties <- partialEntity.properties.asRight[Exception]
      } yield JsonLD.entity(entityId, types, reverse, properties)

    private implicit class OptionOps[T](maybeValue: Option[T]) {
      def toEither(errorMessage: String): Either[Exception, T] = Either.fromOption(
        maybeValue,
        ifNone = new Exception(errorMessage)
      )
    }
  }
}
