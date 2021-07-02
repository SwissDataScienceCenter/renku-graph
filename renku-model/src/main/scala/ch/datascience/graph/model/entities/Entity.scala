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

package ch.datascience.graph.model.entities

import cats.syntax.all._
import ch.datascience.graph.model.Schemas.{prov, renku, wfprov}
import ch.datascience.graph.model.entityModel._
import ch.datascience.graph.model.generations
import io.renku.jsonld.{EntityTypes, JsonLDDecoder}

sealed trait Entity {
  val resourceId: ResourceId
  val location:   Location
  val checksum:   Checksum
}

object Entity {

  final case class InputEntity(resourceId: ResourceId, location: Location, checksum: Checksum) extends Entity
  final case class OutputEntity(resourceId:           ResourceId,
                                location:             Location,
                                checksum:             Checksum,
                                generationResourceId: generations.ResourceId
  ) extends Entity

  import io.renku.jsonld.JsonLDEncoder

  private val fileEntityTypes: EntityTypes = EntityTypes of (prov / "Entity", wfprov / "Artifact")
  private val folderEntityTypes = EntityTypes of (prov / "Entity", wfprov / "Artifact", prov / "Collection")

  implicit def encoder[E <: Entity]: JsonLDEncoder[E] = {
    import ch.datascience.graph.model.Schemas._
    import ch.datascience.graph.model.views.TinyTypeJsonLDEncoders._
    import io.renku.jsonld.syntax._
    import io.renku.jsonld.{JsonLD, JsonLDEncoder}

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
      case entity @ OutputEntity(resourceId, location, checksum, generationResourceId) =>
        JsonLD.entity(
          resourceId.asEntityId,
          toEntityTypes(entity),
          prov / "atLocation"          -> location.asJsonLD,
          renku / "checksum"           -> checksum.asJsonLD,
          prov / "qualifiedGeneration" -> generationResourceId.asEntityId.asJsonLD
        )
    }
  }

  import ch.datascience.graph.model.views.TinyTypeJsonLDDecoders._

  implicit lazy val inputEntityDecoder: JsonLDDecoder[InputEntity] =
    JsonLDDecoder.entity(fileEntityTypes) { cursor =>
      for {
        resourceId  <- cursor.downEntityId.as[ResourceId]
        entityTypes <- cursor.getEntityTypes
        location    <- cursor.downField(prov / "atLocation").as[Location](locationDecoder(entityTypes))
        checksum    <- cursor.downField(renku / "checksum").as[Checksum]
      } yield InputEntity(resourceId, location, checksum)
    }

  implicit lazy val outputEntityDecoder: JsonLDDecoder[OutputEntity] =
    JsonLDDecoder.entity(fileEntityTypes) { cursor =>
      for {
        resourceId            <- cursor.downEntityId.as[ResourceId]
        entityTypes           <- cursor.getEntityTypes
        location              <- cursor.downField(prov / "atLocation").as[Location](locationDecoder(entityTypes))
        checksum              <- cursor.downField(renku / "checksum").as[Checksum]
        qualifiedGenerationId <- cursor.downField(prov / "qualifiedGeneration").as[generations.ResourceId]
      } yield OutputEntity(resourceId, location, checksum, qualifiedGenerationId)
    }

  implicit lazy val entityDecoder: JsonLDDecoder[Entity] =
    cursor => inputEntityDecoder(cursor) orElse outputEntityDecoder(cursor)

  private def locationDecoder(entityTypes: EntityTypes): JsonLDDecoder[Location] =
    JsonLDDecoder.decodeString.emap { value =>
      entityTypes match {
        case `fileEntityTypes`   => Location.File.from(value).leftMap(_.getMessage)
        case `folderEntityTypes` => Location.Folder.from(value).leftMap(_.getMessage)
        case other               => s"Entity with unknown $other types".asLeft
      }
    }
}
