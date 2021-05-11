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

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.{GitLabApiUrl, RenkuBaseUrl}
import ch.datascience.rdfstore.entities.Entity.Checksum
import ch.datascience.tinytypes.constraints.NonBlank
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import io.renku.jsonld._
import io.renku.jsonld.syntax._

sealed trait Entity {
  val location: Location
  val checksum: Checksum
}

object Entity {

  final case class InputEntity(location: Location, checksum: Checksum) extends Entity
  final case class OutputEntity(location: Location, checksum: Checksum, generation: Generation) extends Entity

  object OutputEntity {
    def factory(location: Location): Generation => OutputEntity =
      (generation: Generation) => OutputEntity(location, entityChecksums.generateOne, generation)
  }

  final class Checksum private (val value: String) extends AnyVal with StringTinyType
  object Checksum extends TinyTypeFactory[Checksum](new Checksum(_)) with NonBlank

  implicit def encoder[E <: Entity](implicit renkuBaseUrl: RenkuBaseUrl, gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[E] =
    JsonLDEncoder.instance {
      case entity @ InputEntity(location, checksum) =>
        JsonLD.entity(
          entity.asEntityId,
          EntityTypes of (prov / "Entity", wfprov / "Artifact"),
          prov / "atLocation" -> location.asJsonLD,
          renku / "checksum"  -> checksum.asJsonLD
        )
      case entity @ OutputEntity(location, checksum, generation) =>
        JsonLD.entity(
          entity.asEntityId,
          EntityTypes of (prov / "Entity", wfprov / "Artifact"),
          prov / "atLocation"          -> location.asJsonLD,
          renku / "checksum"           -> checksum.asJsonLD,
          prov / "qualifiedGeneration" -> generation.asEntityId.asJsonLD
        )
    }

  implicit def entityIdEncoder[E <: Entity](implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[E] =
    EntityIdEncoder.instance(entity => EntityId of renkuBaseUrl / "blob" / entity.checksum / entity.location)
}
