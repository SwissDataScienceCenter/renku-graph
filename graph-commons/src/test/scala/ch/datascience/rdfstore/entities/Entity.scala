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

import ch.datascience.graph.config.{GitLabApiUrl, RenkuBaseUrl}
import ch.datascience.rdfstore.entities.Entity.Checksum
import ch.datascience.tinytypes.constraints.NonBlank
import ch.datascience.tinytypes.{StringTinyType, TinyType, TinyTypeFactory}
import io.renku.jsonld._
import io.renku.jsonld.syntax._

import scala.language.postfixOps
import scala.util.Random

final case class Entity(checksum:                  Checksum,
                        location:                  Location,
                        project:                   Project,
                        maybeInvalidationActivity: Option[Activity],
                        maybeGeneration:           Option[Generation]
) extends EntityOps

trait EntityOps {
  self: Entity =>
  lazy val pathIdentifier: List[TinyType] = List(checksum, location)
}

object Entity {

  final class Checksum private (val value: String) extends AnyVal with StringTinyType
  implicit object Checksum extends TinyTypeFactory[Checksum](new Checksum(_)) with NonBlank {
    def generate: Checksum = Checksum(Random.nextString(40))
  }

  def apply(location: Location): Entity =
    Entity(Checksum.generate,
           location,
           generation.activity.project,
           maybeInvalidationActivity = None,
           maybeGeneration = Some(generation)
    )

  def factory(location: Location)(activity: Activity): Entity =
    new Entity(Checksum.generate, location, activity.project, maybeInvalidationActivity = None, maybeGeneration = None)

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[Entity] =
    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        entity.asEntityId,
        EntityTypes of (prov / "Entity", wfprov / "Artifact"),
        rdfs / "label"               -> s"${entity.location}@UNCOMMITTED".asJsonLD,
        prov / "atLocation"          -> entity.location.asJsonLD,
        schema / "isPartOf"          -> entity.project.asJsonLD,
        prov / "qualifiedGeneration" -> entity.maybeGeneration.asJsonLD,
        renku / "checksum"           -> entity.checksum.asJsonLD,
        prov / "wasInvalidatedBy"    -> entity.maybeInvalidationActivity.asJsonLD
      )
    }

  implicit def entityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[Entity] =
    EntityIdEncoder.instance(entity => EntityId of renkuBaseUrl / "blob" / entity.checksum / entity.location)
}
