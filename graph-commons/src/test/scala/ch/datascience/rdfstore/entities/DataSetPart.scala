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

import ch.datascience.graph.model.datasets.{DateCreated, PartLocation, PartName, Url}
import ch.datascience.rdfstore.FusekiBaseUrl
import org.scalacheck.Gen

trait DataSetPart {
  self: Artifact with Entity =>

  val partName:        PartName
  val partDateCreated: DateCreated
  val maybePartUrl:    Option[Url]
}

object DataSetPart {

  import cats.syntax.all._
  import ch.datascience.graph.config.RenkuBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  type DataSetPartArtifact = Artifact with Entity with DataSetPart

  def factory(name: PartName, location: PartLocation, maybeUrl: Option[Url] = None)(
      activity:     Activity
  ): DataSetPartArtifact =
    new Entity(activity.commitId,
               Location(location.value),
               activity.project,
               maybeInvalidationActivity = None,
               maybeGeneration           = None) with Artifact with DataSetPart {
      override val partName:        PartName    = name
      override val partDateCreated: DateCreated = DateCreated(activity.committedDate.value)
      override val maybePartUrl:    Option[Url] = maybeUrl
    }

  private[entities] implicit def converter(implicit renkuBaseUrl: RenkuBaseUrl,
                                           fusekiBaseUrl:         FusekiBaseUrl): PartialEntityConverter[DataSetPartArtifact] =
    new PartialEntityConverter[DataSetPartArtifact] {
      override def convert[T <: DataSetPartArtifact]: T => Either[Exception, PartialEntity] = { entity =>
        PartialEntity(
          EntityTypes of schema / "DigitalDocument",
          schema / "name"        -> entity.partName.asJsonLD,
          schema / "dateCreated" -> entity.partDateCreated.asJsonLD,
          schema / "url"         -> entity.maybePartUrl.asJsonLD
        ).asRight
      }

      override val toEntityId: DataSetPartArtifact => Option[EntityId] = _ => None
    }

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl,
                       fusekiBaseUrl:         FusekiBaseUrl): JsonLDEncoder[DataSetPartArtifact] =
    JsonLDEncoder.instance { entity =>
      entity
        .asPartialJsonLD[Artifact]
        .combine(entity.asPartialJsonLD[Entity])
        .combine(entity.asPartialJsonLD[Artifact])
        .combine(entity.asPartialJsonLD[DataSetPartArtifact])
        .getOrFail
    }

  import ch.datascience.graph.model.GraphModelGenerators.{datasetPartLocations, datasetPartNames}

  lazy val dataSetParts: Gen[(PartName, PartLocation)] = for {
    name     <- datasetPartNames
    location <- datasetPartLocations
  } yield (name, location)
}
