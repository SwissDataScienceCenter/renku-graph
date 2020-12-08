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

import cats.syntax.all._
import ch.datascience.graph.config.{GitLabApiUrl, RenkuBaseUrl}
import ch.datascience.graph.model.datasets._
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.entities.DataSetPart.DataSetPartArtifact
import io.renku.jsonld.EntityId

trait DataSet {
  self: Artifact with Entity =>

  val datasetId:                         Identifier
  val datasetTitle:                      Title
  val datasetName:                       Name
  val datasetUrl:                        Url
  val maybeDatasetSameAs:                Option[SameAs]
  val maybeDatasetDerivedFrom:           Option[DerivedFrom]
  val maybeDatasetDescription:           Option[Description]
  val maybeDatasetPublishedDate:         Option[PublishedDate]
  val datasetCreatedDate:                DateCreated
  val datasetCreators:                   Set[Person]
  val datasetParts:                      List[DataSetPartArtifact]
  val datasetKeywords:                   List[Keyword]
  val overrideDatasetTopmostSameAs:      Option[TopmostSameAs]
  val overrideDatasetTopmostDerivedFrom: Option[TopmostDerivedFrom]

  def topmostSameAs(implicit renkuBaseUrl: RenkuBaseUrl): TopmostSameAs =
    overrideDatasetTopmostSameAs
      .orElse(maybeDatasetSameAs map TopmostSameAs.apply)
      .getOrElse(TopmostSameAs(DataSet entityId datasetId))

  def topmostDerivedFrom(implicit renkuBaseUrl: RenkuBaseUrl): TopmostDerivedFrom =
    overrideDatasetTopmostDerivedFrom
      .orElse(maybeDatasetDerivedFrom map TopmostDerivedFrom.apply)
      .getOrElse(TopmostDerivedFrom(DataSet.entityId(datasetId)))

  def entityId(implicit renkuBaseUrl: RenkuBaseUrl): EntityId = DataSet.entityId(datasetId)
}

object DataSet {

  import ch.datascience.graph.config.RenkuBaseUrl
  import ch.datascience.graph.model.datasets.DerivedFrom._
  import io.renku.jsonld.JsonLDEncoder._
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  type DataSetEntity = Entity with DataSet with Artifact

  def nonModifiedFactory(id:                         Identifier,
                         title:                      Title,
                         name:                       Name,
                         url:                        Url,
                         maybeSameAs:                Option[SameAs] = None,
                         maybeDescription:           Option[Description] = None,
                         maybePublishedDate:         Option[PublishedDate] = None,
                         createdDate:                DateCreated,
                         creators:                   Set[Person],
                         partsFactories:             List[Activity => DataSetPartArtifact],
                         keywords:                   List[Keyword] = Nil,
                         overrideTopmostSameAs:      Option[TopmostSameAs] = None,
                         overrideTopmostDerivedFrom: Option[TopmostDerivedFrom] = None
  )(activity:                                        Activity): DataSetEntity =
    new Entity(activity.commitId,
               Location(".renku") / "datasets" / id,
               activity.project,
               maybeInvalidationActivity = None,
               maybeGeneration = None
    ) with DataSet with Artifact {
      override val datasetId:                         Identifier                 = id
      override val datasetTitle:                      Title                      = title
      override val datasetName:                       Name                       = name
      override val datasetUrl:                        Url                        = url
      override val maybeDatasetSameAs:                Option[SameAs]             = maybeSameAs
      override val maybeDatasetDerivedFrom:           Option[DerivedFrom]        = None
      override val maybeDatasetDescription:           Option[Description]        = maybeDescription
      override val maybeDatasetPublishedDate:         Option[PublishedDate]      = maybePublishedDate
      override val datasetCreatedDate:                DateCreated                = createdDate
      override val datasetCreators:                   Set[Person]                = creators
      override val datasetParts:                      List[DataSetPartArtifact]  = partsFactories.map(_.apply(activity))
      override val datasetKeywords:                   List[Keyword]              = keywords
      override val overrideDatasetTopmostSameAs:      Option[TopmostSameAs]      = overrideTopmostSameAs
      override val overrideDatasetTopmostDerivedFrom: Option[TopmostDerivedFrom] = overrideTopmostDerivedFrom
    }

  def modifiedFactory(id:                         Identifier,
                      title:                      Title,
                      name:                       Name,
                      url:                        Url,
                      derivedFrom:                DerivedFrom,
                      maybeDescription:           Option[Description] = None,
                      maybePublishedDate:         Option[PublishedDate] = None,
                      createdDate:                DateCreated,
                      creators:                   Set[Person],
                      partsFactories:             List[Activity => DataSetPartArtifact],
                      keywords:                   List[Keyword] = Nil,
                      overrideTopmostSameAs:      Option[TopmostSameAs] = None,
                      overrideTopmostDerivedFrom: Option[TopmostDerivedFrom] = None
  )(activity:                                     Activity): DataSetEntity =
    new Entity(activity.commitId,
               Location(".renku") / "datasets" / overrideTopmostDerivedFrom.map(_.value).getOrElse(id.value),
               activity.project,
               maybeInvalidationActivity = None,
               maybeGeneration = None
    ) with DataSet with Artifact {
      override val datasetId:                         Identifier                 = id
      override val datasetTitle:                      Title                      = title
      override val datasetName:                       Name                       = name
      override val datasetUrl:                        Url                        = url
      override val maybeDatasetSameAs:                Option[SameAs]             = None
      override val maybeDatasetDerivedFrom:           Option[DerivedFrom]        = derivedFrom.some
      override val maybeDatasetDescription:           Option[Description]        = maybeDescription
      override val maybeDatasetPublishedDate:         Option[PublishedDate]      = maybePublishedDate
      override val datasetCreatedDate:                DateCreated                = createdDate
      override val datasetCreators:                   Set[Person]                = creators
      override val datasetParts:                      List[DataSetPartArtifact]  = partsFactories.map(_.apply(activity))
      override val datasetKeywords:                   List[Keyword]              = keywords
      override val overrideDatasetTopmostSameAs:      Option[TopmostSameAs]      = overrideTopmostSameAs
      override val overrideDatasetTopmostDerivedFrom: Option[TopmostDerivedFrom] = overrideTopmostDerivedFrom
    }

  def entityId(identifier: DatasetIdentifier)(implicit renkuBaseUrl: RenkuBaseUrl): EntityId =
    EntityId of (renkuBaseUrl / "datasets" / identifier)

  private implicit def converter(implicit
      renkuBaseUrl:  RenkuBaseUrl,
      gitLabApiUrl:  GitLabApiUrl,
      fusekiBaseUrl: FusekiBaseUrl
  ): PartialEntityConverter[DataSetEntity] =
    new PartialEntityConverter[DataSetEntity] {
      override def convert[T <: DataSetEntity]: T => Either[Exception, PartialEntity] = { entity =>
        implicit val creatorsOrdering: Ordering[Person] = Ordering.by((p: Person) => p.name.value)
        PartialEntity(
          entityId(entity.datasetId),
          EntityTypes of schema / "Dataset",
          rdfs / "label"               -> entity.datasetId.asJsonLD,
          schema / "identifier"        -> entity.datasetId.asJsonLD,
          schema / "isPartOf"          -> entity.project.asEntityId.asJsonLD,
          schema / "name"              -> entity.datasetTitle.asJsonLD,
          schema / "alternateName"     -> entity.datasetName.asJsonLD,
          schema / "url"               -> entity.datasetUrl.asJsonLD,
          schema / "sameAs"            -> entity.maybeDatasetSameAs.asJsonLD,
          prov / "wasDerivedFrom"      -> entity.maybeDatasetDerivedFrom.asJsonLD,
          schema / "description"       -> entity.maybeDatasetDescription.asJsonLD,
          schema / "datePublished"     -> entity.maybeDatasetPublishedDate.asJsonLD,
          schema / "dateCreated"       -> entity.datasetCreatedDate.asJsonLD,
          schema / "creator"           -> entity.datasetCreators.asJsonLD,
          schema / "hasPart"           -> entity.datasetParts.asJsonLD,
          schema / "keywords"          -> entity.datasetKeywords.asJsonLD,
          renku / "topmostSameAs"      -> entity.topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> entity.topmostDerivedFrom.asJsonLD
        ).asRight
      }

      override def toEntityId: DataSetEntity => Option[EntityId] = entity => entityId(entity.datasetId).some
    }

  implicit def encoder(implicit
      renkuBaseUrl:  RenkuBaseUrl,
      gitLabApiUrl:  GitLabApiUrl,
      fusekiBaseUrl: FusekiBaseUrl
  ): JsonLDEncoder[DataSetEntity] =
    JsonLDEncoder.instance { entity =>
      entity
        .asPartialJsonLD[Artifact]
        .combine(entity.asPartialJsonLD[Entity])
        .combine(entity.asPartialJsonLD[DataSetEntity])
        .getOrFail
    }
}
