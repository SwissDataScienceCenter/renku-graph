/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.acceptancetests

import ch.datascience.config.renku
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.acceptancetests.tooling.RDFStore
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.{CliVersion, RenkuVersionPair, SchemaVersion}
import ch.datascience.knowledgegraph.projects.ProjectsGenerators._
import ch.datascience.knowledgegraph.projects.model.Project.DateUpdated
import ch.datascience.rdfstore.{FusekiBaseUrl, entities}
import org.scalacheck.Gen

import scala.language.implicitConversions

package object data {
  val renkuResourcesUrl:      renku.ResourcesUrl = renku.ResourcesUrl("http://localhost:9004/knowledge-graph")
  val currentVersionPair:     RenkuVersionPair   = RenkuVersionPair(CliVersion("0.12.2"), SchemaVersion("8"))
  implicit val cliVersion:    CliVersion         = currentVersionPair.cliVersion
  implicit val fusekiBaseUrl: FusekiBaseUrl      = RDFStore.fusekiBaseUrl

  implicit def gitLabProjects[FC <: entities.Project.ForksCount](
      project: entities.Project[FC]
  ): Gen[Project[FC]] = for {
    id               <- projectIds
    maybeDescription <- projectDescriptions.toGeneratorOfOptions
    updatedAt        <- timestamps(min = project.dateCreated.value).toGeneratorOf[DateUpdated]
    urls             <- urlsObjects
    tags             <- tagsObjects.toGeneratorOfSet()
    starsCount       <- starsCounts
    permissions      <- permissionsObjects
    statistics       <- statisticsObjects
  } yield Project(project, id, maybeDescription, updatedAt, urls, tags, starsCount, permissions, statistics)
}
