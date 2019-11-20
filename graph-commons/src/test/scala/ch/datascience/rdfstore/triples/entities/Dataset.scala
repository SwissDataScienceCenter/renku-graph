/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.rdfstore.triples
package entities

import ch.datascience.graph.model.datasets._
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.FilePath
import ch.datascience.graph.model.users.{Affiliation, Email, Name => UserName}
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.triples.entities.Project.`schema:isPartOf`
import ch.datascience.tinytypes.json.TinyTypeEncoders._
import io.circe.Json
import io.circe.literal._

object Dataset {

  def apply(id:                        Id,
            projectId:                 Project.Id,
            datasetName:               Name,
            maybeDatasetDescription:   Option[Description],
            maybeDatasetPublishedDate: Option[PublishedDate],
            maybeDatasetCreators:      Set[(UserName, Option[Email], Option[Affiliation])],
            maybeDatasetParts:         List[(PartName, PartLocation)],
            commitId:                  CommitId,
            maybeDatasetUrl:           Option[String],
            generationPath:            FilePath,
            commitGenerationId:        CommitGeneration.Id)(implicit fusekiBaseUrl: FusekiBaseUrl): List[Json] = {
    val creators = maybeDatasetCreators.toList.map(creator => Person.Id(creator._2) -> creator)
    // format: off
    List(json"""
      {
        "@id": $id,
        "@type": [
          "http://www.w3.org/ns/prov#Entity",
          "http://purl.org/wf4ever/wfprov#Artifact",
          "http://schema.org/Dataset"
        ],
        "http://www.w3.org/ns/prov#atLocation": $generationPath,
        "http://www.w3.org/ns/prov#qualifiedGeneration": {
          "@id": $commitGenerationId
        },
        "http://www.w3.org/2000/01/rdf-schema#label": ${id.datasetId},
        "http://schema.org/identifier": ${id.datasetId},
        "http://schema.org/name": $datasetName
      }""".deepMerge(`schema:isPartOf`(projectId))
          .deepMerge(maybeDatasetUrl toValue "http://schema.org/url")
          .deepMerge(maybeDatasetDescription toValue "http://schema.org/description")
          .deepMerge(maybeDatasetPublishedDate toValue ("http://schema.org/datePublished", "http://schema.org/Date"))
          .deepMerge(creators.map(_._1) toResources "http://schema.org/creator")
          .deepMerge(maybeDatasetParts.map(_._2).map(DatasetPart.Id(commitId, _)) toResources "http://schema.org/hasPart"))
          .++(creators.map { case (id, (name, maybeEmail, maybeAffiliations)) => Person(id, name, maybeEmail, maybeAffiliations) })
          .++(maybeDatasetParts.map { case (name, location) => DatasetPart(DatasetPart.Id(commitId, location), name, projectId) })
    // format: on
  }

  final case class Id(datasetId: Identifier) extends EntityId {
    override val value: String = (renkuBaseUrl / "datasets" / datasetId).toString
  }
}
