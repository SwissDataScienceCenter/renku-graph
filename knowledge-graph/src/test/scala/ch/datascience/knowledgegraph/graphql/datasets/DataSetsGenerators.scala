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

package ch.datascience.knowledgegraph.graphql.datasets

import cats.Order
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.knowledgegraph.graphql.datasets.model._
import eu.timepit.refined.auto._
import org.scalacheck.Gen

object DataSetsGenerators {

  implicit val dataSets: Gen[DataSet] = for {
    id               <- dataSetIds
    name             <- dataSetNames
    maybeDescription <- Gen.option(dataSetDescriptions)
    created          <- dataSetCreations
    published        <- dataSetPublishingInfos
    part             <- listOf(dataSetPart)
    projects         <- nonEmptyList(dataSetProjects)
  } yield DataSet(id, name, maybeDescription, created, published, part, projects.toList)

  private implicit lazy val dataSetAgents: Gen[DataSetAgent] = for {
    email <- emails
    name  <- names
  } yield DataSetAgent(email, name)

  private implicit lazy val dataSetCreations: Gen[DataSetCreation] = for {
    createdDate <- dataSetCreatedDates
    agent       <- dataSetAgents
  } yield DataSetCreation(createdDate, agent)

  private implicit lazy val dataSetCreators: Gen[DataSetCreator] = for {
    maybeEmail <- Gen.option(emails)
    name       <- names
  } yield DataSetCreator(maybeEmail, name)

  implicit lazy val dataSetPublishingInfos: Gen[DataSetPublishing] = for {
    maybePublishedDate <- Gen.option(dataSetPublishedDates)
    creators           <- nonEmptySet(dataSetCreators, maxElements = 4)
  } yield DataSetPublishing(maybePublishedDate, creators.toSortedSet)

  private implicit lazy val dataSetCreatorsOrdering: Order[DataSetCreator] =
    (creator1: DataSetCreator, creator2: DataSetCreator) => creator1.name.value compareTo creator2.name.value

  private implicit lazy val dataSetPart: Gen[DataSetPart] = for {
    name        <- dataSetPartNames
    location    <- dataSetPartLocations
    dateCreated <- dataSetPartCreatedDates
  } yield DataSetPart(name, location, dateCreated)

  implicit lazy val dataSetProjects: Gen[DataSetProject] = for {
    name <- projectPaths
  } yield DataSetProject(name)
}
