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

import ch.datascience.generators.CommonGraphGenerators.{emails, names, schemaVersions}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{httpUrls, listOf, setOf}
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.datasets._
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.graph.model.projects.{FilePath, ProjectPath}
import ch.datascience.graph.model.users.{Email, Name => UserName}
import ch.datascience.rdfstore.triples.entities._
import io.circe.Json
import org.scalacheck.Gen

object singleFileAndCommitWithDataset {

  def apply(projectPath:               ProjectPath,
            commitId:                  CommitId = commitIds.generateOne,
            committerName:             UserName = names.generateOne,
            committerEmail:            Email = emails.generateOne,
            datasetIdentifier:         Identifier = datasetIds.generateOne,
            datasetName:               Name = datasetNames.generateOne,
            maybeDatasetDescription:   Option[Description] = Gen.option(datasetDescriptions).generateOne,
            datasetCreatedDate:        DateCreated = datasetCreatedDates.generateOne,
            maybeDatasetPublishedDate: Option[PublishedDate] = Gen.option(datasetPublishedDates).generateOne,
            maybeDatasetCreators:      Set[(UserName, Option[Email])] = setOf(datasetCreators).generateOne,
            maybeDatasetParts:         List[(PartName, PartLocation)] = listOf(datasetParts).generateOne,
            maybeDatasetUrl:           Option[String] = Gen.option(datasetUrl).generateOne,
            schemaVersion:             SchemaVersion = schemaVersions.generateOne,
            renkuBaseUrl:              RenkuBaseUrl = renkuBaseUrl): List[Json] = {
    val projectId                        = Project.Id(renkuBaseUrl, projectPath)
    val renkuPath                        = FilePath(".renku")
    val renkuCommitCollectionEntityId    = CommitCollectionEntity.Id(commitId, renkuPath)
    val datasetsPath                     = renkuPath / "datasets"
    val datasetsCommitCollectionEntityId = CommitCollectionEntity.Id(commitId, datasetsPath)
    val datasetPath                      = datasetsPath / datasetIdentifier
    val datasetCommitCollectionEntityId  = CommitCollectionEntity.Id(commitId, datasetPath)
    val commitActivityId                 = CommitActivity.Id(commitId)
    val datasetGenerationPath            = FilePath("tree") / datasetPath
    val datasetId                        = Dataset.Id(datasetIdentifier)
    val commitGenerationId               = CommitGeneration.Id(commitId, datasetGenerationPath)
    List(
      Project(projectId),
      CommitActivity(
        commitActivityId,
        projectId,
        Some(Agent.Id(schemaVersion)),
        Some(Person.Id(committerName)),
        maybeInfluencedBy = List(
          renkuCommitCollectionEntityId,
          datasetsCommitCollectionEntityId,
          datasetCommitCollectionEntityId
        )
      ),
      CommitCollectionEntity(renkuCommitCollectionEntityId, projectId, hadMember    = datasetsCommitCollectionEntityId),
      CommitCollectionEntity(datasetsCommitCollectionEntityId, projectId, hadMember = datasetCommitCollectionEntityId),
      CommitCollectionEntity(datasetCommitCollectionEntityId, projectId, hadMember  = datasetId),
      CommitGeneration(commitGenerationId, commitActivityId),
      Agent(schemaVersion),
      Person(Person.Id(committerName), Some(committerEmail))
    ) ++ Dataset(
      datasetId,
      projectId,
      datasetName,
      maybeDatasetDescription,
      datasetCreatedDate,
      maybeDatasetPublishedDate,
      maybeDatasetCreators,
      maybeDatasetParts,
      commitId,
      maybeDatasetUrl,
      datasetGenerationPath,
      commitGenerationId
    )
  }

  private val datasetCreators: Gen[(UserName, Option[Email])] = for {
    name       <- names
    maybeEmail <- Gen.option(emails)
  } yield (name, maybeEmail)

  private val datasetParts: Gen[(PartName, PartLocation)] = for {
    name     <- datasetPartNames
    location <- datasetPartLocations
  } yield (name, location)

  private val datasetUrl: Gen[String] = for {
    url  <- httpUrls
    uuid <- Gen.uuid.map(_.toString)
  } yield s"$url/$uuid"
}
