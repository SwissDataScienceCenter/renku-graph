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

import java.time.Instant

import ch.datascience.graph.model.datasets.{DateCreated, PartLocation, PartName, Url}
import ch.datascience.graph.model.events.CommitId
import ch.datascience.rdfstore.FusekiBaseUrl
import org.scalacheck.Gen

final case class DataSetPart(name:        PartName,
                             location:    PartLocation,
                             commitId:    CommitId,
                             project:     Project,
                             creator:     Person,
                             dateCreated: DateCreated = DateCreated(Instant.now()),
                             maybeUrl:    Option[Url] = None)

object DataSetPart {

  import ch.datascience.graph.config.RenkuBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLDEncoder[DataSetPart] =
    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        EntityId of (fusekiBaseUrl / "blob" / entity.commitId / entity.location),
        EntityTypes of (prov / "Entity", wfprov / "Artifact", schema / "DigitalDocument"),
        prov / "atLocation"    -> entity.location.asJsonLD,
        rdfs / "label"         -> s"${entity.location}@${entity.commitId}".asJsonLD,
        schema / "name"        -> entity.name.asJsonLD,
        schema / "isPartOf"    -> entity.project.asJsonLD,
        schema / "dateCreated" -> entity.dateCreated.asJsonLD,
        schema / "creator"     -> entity.creator.asJsonLD,
        schema / "url"         -> entity.maybeUrl.asJsonLD
      )
    }

  import ch.datascience.graph.model.GraphModelGenerators.{datasetPartLocations, datasetPartNames}

  lazy val dataSetParts: Gen[(PartName, PartLocation)] = for {
    name     <- datasetPartNames
    location <- datasetPartLocations
  } yield (name, location)
}
