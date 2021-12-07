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

package io.renku.graph.model.entities

import cats.syntax.all._
import io.renku.graph.model.Schemas.{prov, renku}
import io.renku.graph.model.entityModel._
import io.renku.graph.model.generations
import io.renku.jsonld.JsonLDDecoder.Result
import io.renku.jsonld.{Cursor, EntityTypes, JsonLDDecoder}

sealed trait Entity {
  val resourceId: ResourceId
  val location:   Location
  val checksum:   Checksum
}

object Entity {

  final case class InputEntity(resourceId: ResourceId, location: Location, checksum: Checksum) extends Entity

  final case class OutputEntity(resourceId:            ResourceId,
                                location:              Location,
                                checksum:              Checksum,
                                generationResourceIds: List[generations.ResourceId]
  ) extends Entity

  import io.renku.jsonld.JsonLDEncoder

  val fileEntityTypes:   EntityTypes = EntityTypes of (prov / "Entity")
  val folderEntityTypes: EntityTypes = EntityTypes of (prov / "Entity", prov / "Collection")

  implicit def encoder[E <: Entity]: JsonLDEncoder[E] = {
    import io.renku.graph.model.Schemas._
    import io.renku.jsonld._
    import io.renku.jsonld.syntax._

    lazy val toEntityTypes: Entity => EntityTypes = { entity =>
      entity.location match {
        case Location.File(_)   => fileEntityTypes
        case Location.Folder(_) => folderEntityTypes
      }
    }

    JsonLDEncoder.instance {
      case entity @ InputEntity(resourceId, location, checksum) =>
        JsonLD.entity(
          resourceId.asEntityId,
          toEntityTypes(entity),
          prov / "atLocation" -> location.asJsonLD,
          renku / "checksum"  -> checksum.asJsonLD
        )
      case entity @ OutputEntity(resourceId, location, checksum, generationResourceIds) =>
        JsonLD.entity(
          resourceId.asEntityId,
          toEntityTypes(entity),
          prov / "atLocation"          -> location.asJsonLD,
          renku / "checksum"           -> checksum.asJsonLD,
          prov / "qualifiedGeneration" -> generationResourceIds.map(_.asEntityId).asJsonLD
        )
    }
  }

  implicit lazy val entityDecoder: JsonLDDecoder[Entity] =
    JsonLDDecoder.entity(fileEntityTypes, withStrictEntityTypes) { cursor =>
      for {
        resourceId         <- cursor.downEntityId.as[ResourceId]
        entityTypes        <- cursor.getEntityTypes
        location           <- cursor.downField(prov / "atLocation").as[Location](locationDecoder(entityTypes))
        checksum           <- cursor.downField(renku / "checksum").as[Checksum]
        maybeGenerationIds <- cursor.downField(prov / "qualifiedGeneration").as[List[generations.ResourceId]]
      } yield maybeGenerationIds match {
        case Nil           => InputEntity(resourceId, location, checksum)
        case generationIds => OutputEntity(resourceId, location, checksum, generationIds)
      }
    }

  def outputEntityDecoder(generationId: generations.ResourceId): JsonLDDecoder[OutputEntity] =
    JsonLDDecoder.entity(
      fileEntityTypes,
      (withStrictEntityTypes &&& withSpecific(generationId)).fmap(_.mapN(_ && _))
    ) { cursor =>
      for {
        resourceId   <- cursor.downEntityId.as[ResourceId]
        entityTypes  <- cursor.getEntityTypes
        location     <- cursor.downField(prov / "atLocation").as[Location](locationDecoder(entityTypes))
        checksum     <- cursor.downField(renku / "checksum").as[Checksum]
        generationId <- cursor.downField(prov / "qualifiedGeneration").as[List[generations.ResourceId]]
      } yield OutputEntity(resourceId, location, checksum, generationId)
    }

  private lazy val withStrictEntityTypes: Cursor => Result[Boolean] =
    _.getEntityTypes.map(types => types == fileEntityTypes || types == folderEntityTypes)

  private def withSpecific(generationId: generations.ResourceId): Cursor => Result[Boolean] =
    _.downField(prov / "qualifiedGeneration")
      .as[List[generations.ResourceId]]
      .map(_.contains(generationId))

  private def locationDecoder(entityTypes: EntityTypes): JsonLDDecoder[Location] =
    JsonLDDecoder.decodeString.emap { value =>
      entityTypes match {
        case types if types contains folderEntityTypes => Location.Folder.from(value).leftMap(_.getMessage)
        case types if types contains fileEntityTypes   => Location.File.from(value).leftMap(_.getMessage)
        case types                                     => s"Entity with unknown $types types".asLeft
      }
    }
}
