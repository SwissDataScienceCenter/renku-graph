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

package ch.datascience.knowledgegraph.datasets

import cats.Order
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.knowledgegraph.datasets.model._
import eu.timepit.refined.auto._
import org.scalacheck.Gen

object DatasetsGenerators {

  implicit val datasets: Gen[Dataset] = for {
    id               <- datasetIds
    name             <- datasetNames
    maybeUrl         <- Gen.option(datasetUrls)
    maybeSameAs      <- Gen.option(datasetSameAs)
    maybeDescription <- Gen.option(datasetDescriptions)
    published        <- datasetPublishingInfos
    part             <- listOf(datasetPart)
    projects         <- nonEmptyList(datasetProjects)
  } yield Dataset(id, name, maybeUrl, maybeSameAs, maybeDescription, published, part, projects.toList)

  implicit lazy val datasetCreators: Gen[DatasetCreator] = for {
    maybeEmail <- Gen.option(emails)
    name       <- names
  } yield DatasetCreator(maybeEmail, name)

  implicit lazy val datasetPublishingInfos: Gen[DatasetPublishing] = for {
    maybePublishedDate <- Gen.option(datasetPublishedDates)
    creators           <- nonEmptySet(datasetCreators, maxElements = 4)
  } yield DatasetPublishing(maybePublishedDate, creators)

  private implicit lazy val datasetCreatorsOrdering: Order[DatasetCreator] =
    (creator1: DatasetCreator, creator2: DatasetCreator) => creator1.name.value compareTo creator2.name.value

  private implicit lazy val datasetPart: Gen[DatasetPart] = for {
    name     <- datasetPartNames
    location <- datasetPartLocations
  } yield DatasetPart(name, location)

  implicit lazy val datasetProjects: Gen[DatasetProject] = for {
    path    <- projectPaths
    name    <- projectNames
    created <- datasetInProjectCreations
  } yield DatasetProject(path, name, created)

  implicit lazy val datasetInProjectCreations: Gen[DatasetInProjectCreation] = for {
    createdDate <- datasetInProjectCreationDates
    agent       <- datasetAgents
  } yield DatasetInProjectCreation(createdDate, agent)

  private implicit lazy val datasetAgents: Gen[DatasetAgent] = for {
    email <- emails
    name  <- names
  } yield DatasetAgent(email, name)
}
