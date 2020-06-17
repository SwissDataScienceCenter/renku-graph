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

import cats.implicits._
import ch.datascience.graph.model.datasets._
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.entities.DataSetPart.DataSetPartArtifact

trait DataSet {
  self: Artifact with Entity =>

  val datasetId:                 Identifier
  val datasetName:               Name
  val maybeDatasetUrl:           Option[Url]
  val maybeDatasetSameAs:        Option[SameAs]
  val maybeDatasetDescription:   Option[Description]
  val maybeDatasetPublishedDate: Option[PublishedDate]
  val datasetCreatedDate:        DateCreated
  val datasetCreators:           Set[Person]
  val datasetParts:              List[DataSetPartArtifact]
  val datasetKeywords:           List[Keyword]
}

object DataSet {

  import ch.datascience.graph.config.RenkuBaseUrl
  import io.renku.jsonld.JsonLDEncoder._
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  type DataSetArtifact = Artifact with Entity with DataSet

  def apply(id:                 Identifier,
            name:               Name,
            maybeUrl:           Option[Url] = None,
            maybeSameAs:        Option[SameAs] = None,
            maybeDescription:   Option[Description] = None,
            maybePublishedDate: Option[PublishedDate] = None,
            createdDate:        DateCreated,
            creators:           Set[Person],
            parts:              List[DataSetPartArtifact],
            generation:         Generation,
            project:            Project,
            keywords:           List[Keyword] = Nil): DataSetArtifact =
    new Entity(generation.activity.id,
               generation.location,
               project,
               maybeInvalidationActivity = None,
               maybeGeneration           = Some(generation)) with Artifact with DataSet {
      override val datasetId:                 Identifier                = id
      override val datasetName:               Name                      = name
      override val maybeDatasetUrl:           Option[Url]               = maybeUrl
      override val maybeDatasetSameAs:        Option[SameAs]            = maybeSameAs
      override val maybeDatasetDescription:   Option[Description]       = maybeDescription
      override val maybeDatasetPublishedDate: Option[PublishedDate]     = maybePublishedDate
      override val datasetCreatedDate:        DateCreated               = createdDate
      override val datasetCreators:           Set[Person]               = creators
      override val datasetParts:              List[DataSetPartArtifact] = parts
      override val datasetKeywords:           List[Keyword]             = keywords
    }

  def entityId(identifier: Identifier)(implicit renkuBaseUrl: RenkuBaseUrl): EntityId =
    EntityId of (renkuBaseUrl / "datasets" / identifier)

  private implicit def converter(
      implicit renkuBaseUrl: RenkuBaseUrl,
      fusekiBaseUrl:         FusekiBaseUrl
  ): PartialEntityConverter[DataSetArtifact] =
    new PartialEntityConverter[DataSetArtifact] {
      override def convert[T <: DataSetArtifact]: T => Either[Exception, PartialEntity] = { entity =>
        implicit val creatorsOrdering: Ordering[Person] = Ordering.by((p: Person) => p.name.value)
        PartialEntity(
          entityId(entity.datasetId),
          EntityTypes of schema / "Dataset",
          rdfs / "label"           -> entity.datasetId.asJsonLD,
          schema / "identifier"    -> entity.datasetId.asJsonLD,
          schema / "name"          -> entity.datasetName.asJsonLD,
          schema / "url"           -> entity.maybeDatasetUrl.asJsonLD,
          schema / "sameAs"        -> entity.maybeDatasetSameAs.asJsonLD,
          schema / "description"   -> entity.maybeDatasetDescription.asJsonLD,
          schema / "datePublished" -> entity.maybeDatasetPublishedDate.asJsonLD,
          schema / "dateCreated"   -> entity.datasetCreatedDate.asJsonLD,
          schema / "creator"       -> entity.datasetCreators.asJsonLD,
          schema / "hasPart"       -> entity.datasetParts.asJsonLD,
          schema / "keywords"      -> entity.datasetKeywords.asJsonLD
        ).asRight
      }
    }

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl,
                       fusekiBaseUrl:         FusekiBaseUrl): JsonLDEncoder[DataSetArtifact] =
    JsonLDEncoder.instance { entity =>
      entity
        .asPartialJsonLD[Artifact]
        .combine(entity.asPartialJsonLD[Entity])
        .combine(entity.asPartialJsonLD[Artifact])
        .combine(entity.asPartialJsonLD[DataSetArtifact])
        .getOrFail
    }
}
