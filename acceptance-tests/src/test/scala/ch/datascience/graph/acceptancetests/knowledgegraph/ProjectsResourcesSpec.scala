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

package ch.datascience.graph.acceptancetests.knowledgegraph

import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.acceptancetests.data._
import ch.datascience.graph.acceptancetests.flows.RdfStoreProvisioning._
import ch.datascience.graph.acceptancetests.knowledgegraph.DatasetsResources.briefJson
import ch.datascience.graph.acceptancetests.stubs.GitLab._
import ch.datascience.graph.acceptancetests.testing.AcceptanceTestPatience
import ch.datascience.graph.acceptancetests.tooling.GraphServices
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.acceptancetests.tooling.TestReadabilityTools._
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.http.client.AccessToken
import ch.datascience.http.rest.Links.{Href, Link, Rel, _links}
import ch.datascience.http.server.EndpointTester._
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.knowledgegraph.projects.ProjectsGenerators.{projects => projectsGen, _}
import ch.datascience.knowledgegraph.projects.model.Project
import ch.datascience.rdfstore.entities.Person
import ch.datascience.rdfstore.entities.bundles._
import io.circe.Json
import io.circe.literal._
import io.renku.jsonld.JsonLD
import org.http4s.Status._
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

class ProjectsResourcesSpec extends FeatureSpec with GivenWhenThen with GraphServices with AcceptanceTestPatience {

  import ProjectsResources._

  private implicit val accessToken: AccessToken = accessTokens.generateOne
  private val project = projectsGen.generateOne.copy(
    maybeDescription = projectDescriptions.generateSome,
    forking          = forkings.generateOne.copy(maybeParent = parentProjects.generateSome),
    permissions      = permissionsObjects.generateOne.copy(maybeGroupAccessLevel = accessLevels.generateSome)
  )
  private val dataset1CommitId = commitIds.generateOne
  private val dataset = datasets.generateOne.copy(
    maybeDescription = Some(datasetDescriptions.generateOne),
    published        = datasetPublishingInfos.generateOne.copy(maybeDate = Some(datasetPublishedDates.generateOne)),
    projects         = List(DatasetProject(project.path, project.name, addedToProject.generateOne))
  )

  feature("GET knowledge-graph/projects/<namespace>/<name> to find project's details") {

    scenario("As a user I would like to find project's details by calling a REST endpoint") {

      Given("some data in the RDF Store")
      val jsonLDTriples = JsonLD.arr(
        dataSetCommit(
          commitId      = dataset1CommitId,
          committer     = Person(project.created.creator.name, project.created.creator.email),
          schemaVersion = currentSchemaVersion
        )(
          project.path,
          projectName        = project.name,
          projectDateCreated = project.created.date
        )(
          datasetIdentifier  = dataset.id,
          datasetName        = dataset.name,
          maybeDatasetSameAs = dataset.sameAs.some
        )
      )

      `data in the RDF store`(project.toGitLabProject(), dataset1CommitId, jsonLDTriples)

      `triples updates run`(Set(project.created.creator.email) + project.created.creator.email)

      And("the project exists in GitLab")
      `GET <gitlab>/api/v4/projects/:path returning OK with`(project, withStatistics = true)

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

  def fullJson(project: Project): Json = {
    val groupAccessLevel = project.permissions.maybeGroupAccessLevel
      .getOrElse(throw new Exception("groupAccessLevel expected"))
    json"""{
      "identifier":  ${project.id.value}, 
      "path":        ${project.path.value}, 
      "name":        ${project.name.value},
      "description": ${(project.maybeDescription getOrElse (throw new Exception("Description expected"))).value},
      "visibility":  ${project.visibility.value},
      "created": {
        "dateCreated": ${project.created.date.value},
        "creator": {
          "name":  ${project.created.creator.name.value},
          "email": ${project.created.creator.email.value}
        }
      },
      "updatedAt":  ${project.updatedAt.value},
      "urls": {
        "ssh":    ${project.urls.ssh.value},
        "http":   ${project.urls.http.value},
        "web":    ${project.urls.web.value},
        "readme": ${project.urls.readme.value}
      },
      "forking": {
        "forksCount": ${project.forking.forksCount.value},
        "parent": {
          "identifier": ${project.forking.maybeParent.getOrElse(throw new Exception("Parent expected")).id.value},
          "path":       ${project.forking.maybeParent.getOrElse(throw new Exception("Parent expected")).path.value},
          "name":       ${project.forking.maybeParent.getOrElse(throw new Exception("Parent expected")).name.value}
        }
      },
      "tags": ${project.tags.map(_.value).toList},
      "starsCount": ${project.starsCount.value},
      "permissions": {
        "projectAccess": {
          "level": {"name": ${project.permissions.projectAccessLevel.name.value}, "value": ${project.permissions.projectAccessLevel.value.value}}
        },
        "groupAccess": {
          "level": {"name": ${groupAccessLevel.name.value}, "value": ${groupAccessLevel.value.value}}
        }
      },
      "statistics": {
        "commitsCount":     ${project.statistics.commitsCount.value},
        "storageSize":      ${project.statistics.storageSize.value},
        "repositorySize":   ${project.statistics.repositorySize.value},
        "lfsObjectsSize":   ${project.statistics.lsfObjectsSize.value},
        "jobArtifactsSize": ${project.statistics.jobArtifactsSize.value}
      }
    }""" deepMerge {
      _links(
        Link(Rel.Self        -> Href(renkuResourcesUrl / "projects" / project.path)),
        Link(Rel("datasets") -> Href(renkuResourcesUrl / "projects" / project.path / "datasets"))
      )
    }
  }
}
