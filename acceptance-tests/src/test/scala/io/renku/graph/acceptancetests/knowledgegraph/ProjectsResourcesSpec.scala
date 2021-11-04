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

package io.renku.graph.acceptancetests.knowledgegraph

import cats.syntax.all._
import io.circe.literal._
import io.circe.{Encoder, Json}
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.data.Project.Permissions._
import io.renku.graph.acceptancetests.data.Project.{Urls, _}
import io.renku.graph.acceptancetests.data.{Project, _}
import io.renku.graph.acceptancetests.flows.RdfStoreProvisioning
import io.renku.graph.acceptancetests.tooling.GraphServices
import io.renku.graph.model.projects.{DateCreated, ForksCount}
import io.renku.graph.model.testentities
import io.renku.graph.model.testentities.Project._
import io.renku.graph.model.testentities._
import io.renku.http.client.AccessToken
import io.renku.http.rest.Links.{Href, Link, Rel, _links}
import io.renku.http.server.EndpointTester.{JsonOps, jsonEntityDecoder}
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import org.http4s.Status._
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec

class ProjectsResourcesSpec
    extends AnyFeatureSpec
    with GivenWhenThen
    with GraphServices
    with RdfStoreProvisioning
    with DatasetsResources
    with RdfStoreData {

  import ProjectsResources._

  private val user = authUsers.generateOne
  private implicit val accessToken: AccessToken = user.accessToken

  private val (dataset, parentProject, project) = {
    val creator = personEntities(withGitLabId, withEmail).generateOne
    val (parent, child) = projectEntities(visibilityPublic).generateOne
      .copy(maybeCreator = creator.some, members = personEntities(withGitLabId).generateFixedSizeSet() + creator)
      .forkOnce()

    val (dataset, parentWithDataset) = parent.addDataset(datasetEntities(provenanceInternal))

    (dataset,
     dataProjects(parentWithDataset).generateOne,
     dataProjects(
       child.copy(visibility = visibilityNonPublic.generateOne,
                  members = child.members + personEntities.generateOne.copy(maybeGitLabId = user.id.some)
       )
     ).generateOne
    )
  }

  Feature("GET knowledge-graph/projects/<namespace>/<name> to find project's details") {

    Scenario("As a user I would like to find project's details by calling a REST endpoint") {

      Given("I am authenticated")
      `GET <gitlabApi>/user returning OK`(user)

      Given("some data in the RDF Store")

      `data in the RDF store`(parentProject, parentProject.entitiesProject.asJsonLD)
      `wait for events to be processed`(parentProject.id)

      `data in the RDF store`(project, JsonLD.arr(dataset.asJsonLD, project.entitiesProject.asJsonLD))
      `wait for events to be processed`(project.id)

      When("user fetches project's details with GET knowledge-graph/projects/<namespace>/<name>")
      val projectDetailsResponse = knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.path}", accessToken)

      Then("he should get OK response with project's details")
      projectDetailsResponse.status shouldBe Ok
      val projectDetails = projectDetailsResponse.jsonBody
      projectDetails shouldBe fullJson(project)

      When("user then fetches project's datasets using the link from the response")
      val datasetsLink = projectDetails._links.fold(throw _, identity).get(Rel("datasets")) getOrElse fail(
        "No link with rel 'datasets'"
      )
      val datasetsResponse = (restClient GET datasetsLink.href.toString)
        .flatMap(response => response.as[Json].map(json => response.status -> json))
        .unsafeRunSync()

      Then("he should get OK response with the projects datasets")
      datasetsResponse._1 shouldBe Ok
      val Right(foundDatasets) = datasetsResponse._2.as[List[Json]]
      foundDatasets should contain theSameElementsAs List(briefJson(dataset, project.path))

      When("there's an authenticated user who is not project member")
      val nonMemberAccessToken = accessTokens.generateOne
      `GET <gitlabApi>/user returning OK`()(nonMemberAccessToken)

      And("he fetches project's details")
      val projectDetailsResponseForNonMember =
        knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.path}", nonMemberAccessToken)

      Then("he should get NOT_FOUND response")
      projectDetailsResponseForNonMember.status shouldBe Ok // NotFound after the security is back
    }
  }
}

object ProjectsResources {

  def fullJson(project: Project): Json = json"""{
    "identifier":  ${project.id.value}, 
    "path":        ${project.path.value}, 
    "name":        ${project.name.value},
    "visibility":  ${project.entitiesProject.visibility.value},
    "created":     ${(project.entitiesProject.dateCreated, project.entitiesProject.maybeCreator)},
    "updatedAt":   ${project.updatedAt.value},
    "urls":        ${project.urls.toJson},
    "forking":     ${project.entitiesProject.forksCount -> project.entitiesProject},
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
    "version": ${project.entitiesProject.version.value}
  }""" deepMerge {
    _links(
      Link(Rel.Self        -> Href(renkuResourcesUrl / "projects" / project.path)),
      Link(Rel("datasets") -> Href(renkuResourcesUrl / "projects" / project.path / "datasets"))
    )
  } deepMerge {
    project.entitiesProject.maybeDescription
      .map(description => json"""{"description": ${description.value} }""")
      .getOrElse(Json.obj())
  }

  private implicit class UrlsOps(urls: Urls) {
    import io.renku.json.JsonOps.JsonOps

    lazy val toJson: Json = json"""{
      "ssh":       ${urls.ssh.value},
      "http":      ${urls.http.value},
      "web":       ${urls.web.value}
    }""" addIfDefined ("readme" -> urls.maybeReadme.map(_.value))
  }

  private implicit lazy val forkingEncoder: Encoder[(ForksCount, testentities.Project)] =
    Encoder.instance {
      case (forksCount, project: testentities.ProjectWithParent) => json"""{
      "forksCount": ${forksCount.value},
      "parent": {
        "path":    ${project.parent.path.value},
        "name":    ${project.parent.name.value},
        "created": ${(project.parent.dateCreated, project.parent.maybeCreator)}
      }
    }"""
      case (forksCount, _) => json"""{
      "forksCount": ${forksCount.value}
    }"""
    }

  private implicit lazy val createdEncoder: Encoder[(DateCreated, Option[Person])] = Encoder.instance {
    case (dateCreated, Some(creator)) => json"""{
      "dateCreated": ${dateCreated.value},
      "creator": $creator
    }"""
    case (dateCreated, _) => json"""{
      "dateCreated": ${dateCreated.value}
    }"""
  }

  private implicit lazy val personEncoder: Encoder[Person] = Encoder.instance {
    case Person(name, Some(email), _, _) => json"""{
      "name": ${name.value},
      "email": ${email.value}
    }"""
    case Person(name, _, _, _) => json"""{
      "name": ${name.value}
    }"""
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
    case ProjectPermissions(projectAccessLevel) => json"""{
      "projectAccess": {
        "level": {"name": ${projectAccessLevel.name.value}, "value": ${projectAccessLevel.value.value}}
      }
    }"""
    case GroupPermissions(groupAccessLevel) => json"""{
      "groupAccess": {
        "level": {"name": ${groupAccessLevel.name.value}, "value": ${groupAccessLevel.value.value}}
      }
    }"""
  }
}
