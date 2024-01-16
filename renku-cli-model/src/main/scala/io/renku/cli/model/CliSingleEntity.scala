/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.cli.model

import io.renku.cli.model.Ontologies.{Prov, Renku}
import io.renku.graph.model.entityModel._
import io.renku.graph.model.generations
import io.renku.jsonld.JsonLDDecoder.Result
import io.renku.jsonld._
import io.renku.jsonld.syntax._

final case class CliSingleEntity(
    resourceId:    ResourceId,
    path:          EntityPath,
    checksum:      Checksum,
    generationIds: List[generations.ResourceId]
) extends CliModel

object CliSingleEntity {

  private val entityTypes: EntityTypes = EntityTypes.of(Prov.Entity)

  private[model] def matchingEntityTypes(entityTypes: EntityTypes): Boolean =
    entityTypes == this.entityTypes

  private val withStrictEntityTypes: Cursor => Result[Boolean] =
    _.getEntityTypes.map(types => types == entityTypes)

  implicit def jsonLDDecoder: JsonLDDecoder[CliSingleEntity] =
    JsonLDDecoder.cacheableEntity(entityTypes, withStrictEntityTypes) { cursor =>
      for {
        resourceId    <- cursor.downEntityId.as[ResourceId]
        path          <- cursor.downField(Prov.atLocation).as[EntityPath]
        checksum      <- cursor.downField(Renku.checksum).as[Checksum]
        generationIds <- cursor.downField(Prov.qualifiedGeneration).as[List[generations.ResourceId]]
      } yield CliSingleEntity(resourceId, path, checksum, generationIds)
    }

  implicit def jsonLDEncoder: JsonLDEncoder[CliSingleEntity] =
    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        entity.resourceId.asEntityId,
        entityTypes,
        Prov.atLocation          -> entity.path.asJsonLD,
        Renku.checksum           -> entity.checksum.asJsonLD,
        Prov.qualifiedGeneration -> entity.generationIds.asJsonLD
      )
    }
}
