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
import ch.datascience.graph.model.projects.Path
import ch.datascience.http.client.AccessToken
import ch.datascience.http.rest.Links.{Href, Link, Rel, _links}
import ch.datascience.http.server.EndpointTester._
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.knowledgegraph.projects.ProjectsGenerators._
import ch.datascience.knowledgegraph.projects.model.Permissions.{GroupPermissions, ProjectAndGroupPermissions, ProjectPermissions}
import ch.datascience.knowledgegraph.projects.model._
import ch.datascience.rdfstore.entities
import ch.datascience.rdfstore.entities.Person
import ch.datascience.rdfstore.entities.bundles._
import io.circe.Json
import io.circe.literal._
import io.renku.jsonld.JsonLD
import org.http4s.Status._
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should

class ProjectsResourcesSpec
    extends AnyFeatureSpec
    with GivenWhenThen
    with GraphServices
    with AcceptanceTestPatience
    with should.Matchers {

  import ProjectsResources._

  private implicit val accessToken: AccessToken = accessTokens.generateOne
  private val parentProject = {
    val initParent = parents.generateOne
    ParentProject(
      initParent.resourceId.toUnsafe[Path],
      initParent.name,
      Creation(initParent.created.date,
               initParent.created.maybeCreator.map(creator => Creator(creator.maybeEmail, creator.name)))
    )
  }
  private val project = {
    val initProject = projects.generateOne
    initProject.copy(
      maybeDescription = projectDescriptions.generateSome,
      forking = initProject.forking.copy(
        maybeParent = parentProject.some
      )
    )
  }
  private val dataset1CommitId = commitIds.generateOne
  private val dataset = nonModifiedDatasets().generateOne.copy(
    maybeDescription = Some(datasetDescriptions.generateOne),
    published        = datasetPublishingInfos.generateOne.copy(maybeDate = Some(datasetPublishedDates.generateOne)),
    projects         = List(DatasetProject(project.path, project.name, addedToProjectObjects.generateOne))
  )

  Feature("GET knowledge-graph/projects/<namespace>/<name> to find project's details") {

    Scenario("As a user I would like to find project's details by calling a REST endpoint") {

      Given("some data in the RDF Store")
      val jsonLDTriples = JsonLD.arr(
        nonModifiedDataSetCommit(
          commitId   = dataset1CommitId,
          cliVersion = currentCliVersion
        )(
          projectPath         = project.path,
          projectName         = project.name,
          projectDateCreated  = project.created.date,
          maybeProjectCreator = project.created.maybeCreator.map(creator => Person(creator.name, creator.maybeEmail)),
          maybeParent = entities
            .Project(
              parentProject.path,
              parentProject.name,
              parentProject.created.date,
              maybeCreator =
                parentProject.created.maybeCreator.map(creator => entities.Person(creator.name, creator.maybeEmail)),
              version = projectSchemaVersions.generateOne
            )
            .some,
          projectVersion = project.version
        )(
          datasetIdentifier  = dataset.id,
          datasetTitle       = dataset.title,
          datasetName        = dataset.name,
          maybeDatasetSameAs = dataset.sameAs.some
        )
      )
      `data in the RDF store`(project, dataset1CommitId, jsonLDTriples)

      `triples updates run`(
        Set(project.created.maybeCreator.flatMap(_.maybeEmail),
            parentProject.created.maybeCreator.flatMap(_.maybeEmail)).flatten
      )

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

  def fullJson(project: Project): Json = json"""{
    "identifier":  ${project.id.value}, 
    "path":        ${project.path.value}, 
    "name":        ${project.name.value},
    "description": ${(project.maybeDescription getOrElse (throw new Exception("Description expected"))).value},
    "visibility":  ${project.visibility.value},
    "created":     ${project.created.toJson},
    "updatedAt":   ${project.updatedAt.value},
    "urls": {
      "ssh":       ${project.urls.ssh.value},
      "http":      ${project.urls.http.value},
      "web":       ${project.urls.web.value},
      "readme":    ${project.urls.maybeReadme.map(_.value)}
    },
    "forking":     ${project.forking.toJson},
    "tags":        ${project.tags.map(_.value).toList},
    "starsCount":  ${project.starsCount.value},
    "permissions": ${toJson(project.permissions)},
    "statistics": {
      "commitsCount":     ${project.statistics.commitsCount.value},
      "storageSize":      ${project.statistics.storageSize.value},
      "repositorySize":   ${project.statistics.repositorySize.value},
      "lfsObjectsSize":   ${project.statistics.lsfObjectsSize.value},
      "jobArtifactsSize": ${project.statistics.jobArtifactsSize.value}
    },
    "version": ${project.version.value}
  }""" deepMerge {
    _links(
      Link(Rel.Self        -> Href(renkuResourcesUrl / "projects" / project.path)),
      Link(Rel("datasets") -> Href(renkuResourcesUrl / "projects" / project.path / "datasets"))
    )
  }

  private implicit class CreationOps(created: Creation) {
    import ch.datascience.json.JsonOps._

    lazy val toJson: Json = json"""{
      "dateCreated": ${created.date.value}
    }""" addIfDefined ("creator" -> created.maybeCreator.map(_.toJson))
  }

  private implicit class ForkingOps(forking: Forking) {
    import ch.datascience.json.JsonOps._

    lazy val toJson: Json = json"""{
      "forksCount": ${forking.forksCount.value}
    }""" addIfDefined ("parent" -> forking.maybeParent.map(_.toJson))
  }

  private implicit class ParentOps(parent: ParentProject) {
    lazy val toJson: Json = json"""{
      "path":    ${parent.path.value},
      "name":    ${parent.name.value},
      "created": ${parent.created.toJson}
    }"""
  }

  private implicit class ProjectOps(project: Project) {
    lazy val parent: ParentProject = project.forking.maybeParent.getOrElse(throw new Exception("Parent expected"))
  }

  private implicit class CreatorOps(creator: Creator) {
    import ch.datascience.json.JsonOps._

    lazy val toJson: Json = json"""{
      "name":  ${creator.name.value}
    }""" addIfDefined ("email" -> creator.maybeEmail.map(_.value))
  }

  private lazy val toJson: Permissions => Json = {
    case ProjectAndGroupPermissions(projectAccessLevel, groupAccessLevel) => json"""{
      "projectAccess": {
        "level": {"name": ${projectAccessLevel.name.value}, "value": ${projectAccessLevel.value.value}}
      },
      "groupAccess": {
        "level": {"name": ${groupAccessLevel.name.value}, "value": ${groupAccessLevel.value.value}}
      }
    }"""
    case ProjectPermissions(projectAccessLevel)                           => json"""{
      "projectAccess": {
        "level": {"name": ${projectAccessLevel.name.value}, "value": ${projectAccessLevel.value.value}}
      }
    }"""
    case GroupPermissions(groupAccessLevel)                               => json"""{
      "groupAccess": {
        "level": {"name": ${groupAccessLevel.name.value}, "value": ${groupAccessLevel.value.value}}
      }
    }"""
  }
}
