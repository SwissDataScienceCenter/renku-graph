/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.graph.model.testentities

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import io.renku.graph.model.entityModel.{Checksum, Location}
import io.renku.jsonld._
import io.renku.jsonld.syntax._

sealed trait Entity {
  val location: Location
  val checksum: Checksum
}

object Entity {

  final case class InputEntity(location: Location, checksum: Checksum)                          extends Entity
  final case class OutputEntity(location: Location, checksum: Checksum, generation: Generation) extends Entity

  object OutputEntity {
    def factory(location: Location): Generation => OutputEntity =
      (generation: Generation) => OutputEntity(location, entityChecksums.generateOne, generation)
  }

  implicit def toEntity(implicit renkuUrl: RenkuUrl): Entity => entities.Entity = {
    case e: InputEntity  => toInputEntity(renkuUrl)(e)
    case e: OutputEntity => toOutputEntity(renkuUrl)(e)
  }

  implicit def toInputEntity(implicit renkuUrl: RenkuUrl): InputEntity => entities.Entity.InputEntity =
    entity =>
      entities.Entity.InputEntity(entityModel.ResourceId(entity.asEntityId.show), entity.location, entity.checksum)

  implicit def toOutputEntity(implicit renkuUrl: RenkuUrl): OutputEntity => entities.Entity.OutputEntity =
    entity =>
      entities.Entity.OutputEntity(entityModel.ResourceId(entity.asEntityId.show),
                                   entity.location,
                                   entity.checksum,
                                   List(generations.ResourceId(entity.generation.asEntityId.show))
      )

  implicit def encoder[E <: Entity](implicit renkuUrl: RenkuUrl): JsonLDEncoder[E] =
    JsonLDEncoder.instance(_.to[entities.Entity].asJsonLD)

  implicit def entityIdEncoder[E <: Entity](implicit renkuUrl: RenkuUrl): EntityIdEncoder[E] =
    EntityIdEncoder.instance(entity => EntityId of renkuUrl / "blob" / entity.checksum / entity.location)
}
