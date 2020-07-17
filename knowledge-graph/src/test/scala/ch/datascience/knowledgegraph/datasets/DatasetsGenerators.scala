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

package ch.datascience.knowledgegraph.datasets

import cats.Order
import cats.data.NonEmptyList
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{DerivedFrom, SameAs, Url}
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.rdfstore.entities.DataSet
import eu.timepit.refined.auto._
import org.scalacheck.Gen

object DatasetsGenerators {

  implicit val datasets: Gen[Dataset] = nonModifiedDatasets()

  def nonModifiedDatasets(
      sameAs:   Gen[SameAs]                       = datasetSameAs,
      projects: Gen[NonEmptyList[DatasetProject]] = nonEmptyList(datasetProjects)
  ): Gen[NonModifiedDataset] =
    for {
      id               <- datasetIdentifiers
      name             <- datasetNames
      url              <- datasetUrls
      sameAs           <- sameAs
      maybeDescription <- Gen.option(datasetDescriptions)
      published        <- datasetPublishingInfos
      part             <- listOf(datasetParts)
      projects         <- projects
    } yield NonModifiedDataset(id, name, url, sameAs, maybeDescription, published, part, projects.toList)

  def modifiedDatasets(dataset: Dataset, project: DatasetProject, derivedFromOverride: Option[DerivedFrom] = None)(
      implicit renkuBaseUrl:    RenkuBaseUrl
  ): Gen[ModifiedDataset] =
    for {
      id        <- datasetIdentifiers
      published <- datasetPublishingInfos
    } yield ModifiedDataset(
      id,
      dataset.name,
      Url(DataSet.entityId(id).toString),
      derivedFromOverride getOrElse DerivedFrom(DataSet.entityId(dataset.id)),
      dataset.maybeDescription,
      published,
      dataset.parts,
      List(project)
    )

  implicit lazy val datasetCreators: Gen[DatasetCreator] = for {
    maybeEmail       <- Gen.option(userEmails)
    name             <- userNames
    maybeAffiliation <- Gen.option(userAffiliations)
  } yield DatasetCreator(maybeEmail, name, maybeAffiliation)

  implicit lazy val datasetPublishingInfos: Gen[DatasetPublishing] = for {
    maybePublishedDate <- Gen.option(datasetPublishedDates)
    creators           <- nonEmptySet(datasetCreators, maxElements = 4)
  } yield DatasetPublishing(maybePublishedDate, creators)

  private implicit lazy val datasetCreatorsOrdering: Order[DatasetCreator] =
    (creator1: DatasetCreator, creator2: DatasetCreator) => creator1.name.value compareTo creator2.name.value

  private implicit lazy val datasetParts: Gen[DatasetPart] = for {
    name     <- datasetPartNames
    location <- datasetPartLocations
  } yield DatasetPart(name, location)

  implicit lazy val datasetProjects: Gen[DatasetProject] = for {
    path    <- projectPaths
    name    <- projectNames
    created <- addedToProjectObjects
  } yield DatasetProject(path, name, created)

  implicit lazy val addedToProjectObjects: Gen[AddedToProject] = for {
    createdDate <- datasetInProjectCreationDates
    agent       <- datasetAgents
  } yield AddedToProject(createdDate, agent)

  private implicit lazy val datasetAgents: Gen[DatasetAgent] = for {
    maybeEmail <- userEmails.toGeneratorOfOptions
    name       <- userNames
  } yield DatasetAgent(maybeEmail, name)
}
