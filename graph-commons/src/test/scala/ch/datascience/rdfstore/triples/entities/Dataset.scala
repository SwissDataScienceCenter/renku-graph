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
import ch.datascience.graph.model.users.{Email, Name => UserName}
import ch.datascience.rdfstore.triples.entities.Project.`schema:isPartOf`
import ch.datascience.tinytypes.json.TinyTypeEncoders._
import io.circe.Json
import io.circe.literal._

private[triples] object Dataset {

  // format: off
  def apply(
      id:                        Id,
      projectId:                 Project.Id,
      datasetName:               Name,
      maybeDatasetDescription:   Option[Description],
      datasetCreatedDate:        DateCreated,
      maybeDatasetPublishedDate: Option[PublishedDate],
      maybeDatasetCreators:      Set[(UserName, Option[Email])],
      maybeDatasetParts:         List[(PartName, PartLocation, PartDateCreated)],
      commitId:                  CommitId,
      maybeDatasetUrl:           Option[String],
      generationPath:            FilePath,
      commitGenerationId:        CommitGeneration.Id
  ): List[Json] = List(json"""
  {
    "@id": $id,
    "@type": [
      "prov:Entity",
      "http://purl.org/wf4ever/wfprov#Artifact",
      "schema:Dataset"
    ],
    "prov:atLocation": $generationPath,
    "prov:qualifiedGeneration": {
      "@id": $commitGenerationId
    },
    "rdfs:label": ${id.datasetId},
    "schema:identifier": ${id.datasetId},
    "schema:name": $datasetName,
    "schema:dateCreated": $datasetCreatedDate
  }"""
    .deepMerge(`schema:isPartOf`(projectId))
    .deepMerge(maybeDatasetUrl to "schema:url")
    .deepMerge(maybeDatasetDescription to "schema:description")
    .deepMerge(maybeDatasetPublishedDate to ("schema:datePublished", "schema:Date"))
    .deepMerge(maybeDatasetCreators.map(_._1).map(Person.Id).toList toResources "schema:creator")
    .deepMerge(maybeDatasetParts.map(_._2).map(DatasetPart.Id(commitId, _)) toResources "schema:hasPart")
  )
    .++(maybeDatasetCreators.toList.map { case (name, maybeEmail) => Person(Person.Id(name), maybeEmail) })
    .++(maybeDatasetParts.map { case (name, location, dateCreated) => DatasetPart(DatasetPart.Id(commitId, location), name, dateCreated, projectId) })
  // format: on

  final case class Id(datasetId: Identifier) extends EntityId {
    override val value: String = s"file:///$datasetId"
  }
}
