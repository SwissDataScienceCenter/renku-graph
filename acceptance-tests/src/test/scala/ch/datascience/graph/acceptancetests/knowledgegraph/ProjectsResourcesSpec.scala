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

package ch.datascience.graph.acceptancetests.knowledgegraph

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.acceptancetests.data._
import ch.datascience.graph.acceptancetests.flows.RdfStoreProvisioning.`data in the RDF store`
import ch.datascience.graph.acceptancetests.knowledgegraph.DatasetsResources.briefJson
import ch.datascience.graph.acceptancetests.testing.AcceptanceTestPatience
import ch.datascience.graph.acceptancetests.tooling.GraphServices
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.acceptancetests.tooling.TestReadabilityTools._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.EventsGenerators.commitIds
import ch.datascience.http.rest.Links.{Href, Link, Rel, _links}
import ch.datascience.http.server.EndpointTester._
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.knowledgegraph.projects.ProjectsGenerators.{projects => projectsGen}
import ch.datascience.knowledgegraph.projects.model.Project
import ch.datascience.rdfstore.triples.{singleFileAndCommitWithDataset, triples}
import io.circe.Json
import io.circe.literal._
import org.http4s.Status._
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

class ProjectsResourcesSpec extends FeatureSpec with GivenWhenThen with GraphServices with AcceptanceTestPatience {

  import ProjectsResources._

  private val project          = projectsGen.generateOne
  private val dataset1CommitId = commitIds.generateOne
  private val dataset = datasets.generateOne.copy(
    maybeDescription = Some(datasetDescriptions.generateOne),
    published        = datasetPublishingInfos.generateOne.copy(maybeDate = Some(datasetPublishedDates.generateOne)),
    project          = List(DatasetProject(project.path, project.name))
  )

  feature("GET knowledge-graph/projects/<namespace>/<name> to find project's details") {

    scenario("As a user I would like to find project's details by calling a REST endpoint") {

      Given("some data in the RDF Store")
      val jsonLDTriples = triples(
        singleFileAndCommitWithDataset(
          project.path,
          projectName        = project.name,
          projectDateCreated = project.created.date,
          projectCreator     = project.created.creator.name -> project.created.creator.email,
          commitId           = dataset1CommitId,
          datasetIdentifier  = dataset.id,
          datasetName        = dataset.name,
          schemaVersion      = currentSchemaVersion
        )
      )

      `data in the RDF store`(project.toGitLabProject(), dataset1CommitId, jsonLDTriples)

      When("user fetches project's details with GET knowledge-graph/projects/<namespace>/<name>")
      val projectDetailsResponse = knowledgeGraphClient GET s"knowledge-graph/projects/${project.path}"

      Then("he should get OK response with project's details")
      projectDetailsResponse.status shouldBe Ok
      val Right(projectDetails) = projectDetailsResponse.bodyAsJson.as[Json]
      projectDetails shouldBe fullJson(project)

      When("user then fetches project's datasets using the link from the response")
      val datasetsLink     = projectDetails._links.get(Rel("datasets")) getOrFail (message = "No link with rel 'datasets'")
      val datasetsResponse = restClient GET datasetsLink.toString

      Then("he should get OK response with the projects datasets")
      datasetsResponse.status shouldBe Ok
      val Right(foundDatasets) = datasetsResponse.bodyAsJson.as[List[Json]]
      foundDatasets should contain theSameElementsAs List(briefJson(dataset))
    }
  }
}

object ProjectsResources {

  def fullJson(project: Project): Json = json"""
    {
      "path": ${project.path.toString}, 
      "name": ${project.name.toString},
      "created": {
        "dateCreated": ${project.created.date.toString},
        "creator": {
          "name": ${project.created.creator.name.toString},
          "email": ${project.created.creator.email.toString}
        }
      }
    }""" deepMerge {
    _links(
      Link(Rel.Self        -> Href(renkuResourceUrl / "projects" / project.path.value)),
      Link(Rel("datasets") -> Href(renkuResourceUrl / "projects" / project.path.value / "datasets"))
    )
  }
}
