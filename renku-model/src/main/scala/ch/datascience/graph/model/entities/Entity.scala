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

import ch.datascience.graph.model.entityModel._
import io.renku.jsonld.EntityTypes

sealed trait Entity {
  val resourceId: ResourceId
  val location:   Location
  val checksum:   Checksum
}

object Entity {

  final case class InputEntity(resourceId: ResourceId, location: Location, checksum: Checksum) extends Entity
  final case class OutputEntity(resourceId: ResourceId, location: Location, checksum: Checksum, generation: Generation)
      extends Entity

  import io.renku.jsonld.JsonLDEncoder

  implicit def encoder[E <: Entity]: JsonLDEncoder[E] = {
    import ch.datascience.graph.model.Schemas._
    import ch.datascience.graph.model.views.TinyTypeJsonLDEncoders._
    import io.renku.jsonld.syntax._
    import io.renku.jsonld.{JsonLD, JsonLDEncoder}

    lazy val toEntityTypes: Entity => EntityTypes = { entity =>
      entity.location match {
        case Location.File(_)   => EntityTypes of (prov / "Entity", wfprov / "Artifact")
        case Location.Folder(_) => EntityTypes of (prov / "Entity", wfprov / "Artifact", prov / "Collection")
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
      case entity @ OutputEntity(resourceId, location, checksum, generation) =>
        JsonLD.entity(
          resourceId.asEntityId,
          toEntityTypes(entity),
          prov / "atLocation"          -> location.asJsonLD,
          renku / "checksum"           -> checksum.asJsonLD,
          prov / "qualifiedGeneration" -> generation.resourceId.asEntityId.asJsonLD
        )
    }
  }
}
