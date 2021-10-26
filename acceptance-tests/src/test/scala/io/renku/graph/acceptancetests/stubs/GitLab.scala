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

package io.renku.graph.acceptancetests.stubs

import cats.syntax.all._
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.client.{MappingBuilder, WireMock}
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import com.github.tomakehurst.wiremock.http.Fault.CONNECTION_RESET_BY_PEER
import com.github.tomakehurst.wiremock.stubbing.StubMapping
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.acceptancetests.data
import io.renku.graph.acceptancetests.data.Project.Permissions
import io.renku.graph.acceptancetests.data.Project.Permissions._
import io.renku.graph.acceptancetests.tooling.{GraphServices, TestLogger}
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.projects.Id
import io.renku.graph.model.testentities.{Person, ProjectWithParent}
import io.renku.graph.model.{GitLabApiUrl, GitLabUrl, users}
import io.renku.http.client.AccessToken
import io.renku.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import io.renku.http.client.UrlEncoder.urlEncode
import io.renku.http.server.security.model.AuthUser

import java.time.Instant

trait GitLab {
  self: GraphServices =>

  import GitLabWiremockInstance._

  private val port:               Int Refined Positive = GitLabWiremockInstance.port
  implicit lazy val gitLabUrl:    GitLabUrl            = GitLabUrl(s"http://localhost:$port")
  implicit lazy val gitLabApiUrl: GitLabApiUrl         = gitLabUrl.apiV4

  def `GET <gitlabApi>/user returning OK`(user: AuthUser): Unit =
    `GET <gitlabApi>/user returning OK`(user.id)(user.accessToken)

  def `GET <gitlabApi>/user returning OK`(
      userGitLabId:       users.GitLabId = userGitLabIds.generateOne
  )(implicit accessToken: AccessToken): Unit = {
    stubFor {
      get("/api/v4/user").withAccessTokenInHeader
        .willReturn(okJson(json"""{
          "id": ${userGitLabId.value} 
        }""".noSpaces))
    }
    ()
  }

  def `GET <gitlabApi>/projects/:id/hooks returning OK with the hook`(
      projectId:          Id
  )(implicit accessToken: AccessToken): Unit = {
    val webhookUrl = s"${webhookServiceClient.baseUrl}/webhooks/events"
    stubFor {
      get(s"/api/v4/projects/$projectId/hooks").withAccessTokenInHeader
        .willReturn(okJson(json"""[{"url": $webhookUrl}]""".noSpaces))
    }
    ()
  }

  def `GET <gitlabApi>/projects/:id/hooks returning OK with no hooks`(
      projectId:          Id
  )(implicit accessToken: AccessToken): Unit = {
    stubFor {
      get(s"/api/v4/projects/$projectId/hooks").withAccessTokenInHeader
        .willReturn(okJson(json"""[]""".noSpaces))
    }
    ()
  }

  def `POST <gitlabApi>/projects/:id/hooks returning CREATED`(
      projectId:          Id
  )(implicit accessToken: AccessToken): Unit = {
    stubFor {
      post(s"/api/v4/projects/$projectId/hooks").withAccessTokenInHeader
        .willReturn(created())
    }
    ()
  }

  def `GET <gitlabApi>/projects/:id/repository/commits per page returning OK with a commit`(
      projectId:          Id,
      commitIds:          CommitId*
  )(implicit accessToken: AccessToken): Unit = {
    val theMostRecentEventDate = Instant.now()
    stubFor {
      get(s"/api/v4/projects/$projectId/repository/commits")
        .willReturn(okJson(commitIds.map(commitAsJson(_, theMostRecentEventDate)).asJson.noSpaces))
        .withAccessTokenInHeader
    }
    stubFor {
      get(s"/api/v4/projects/$projectId/repository/commits?per_page=1")
        .willReturn(okJson(Json.arr(commitAsJson(commitIds.last, theMostRecentEventDate)).noSpaces))
        .withAccessTokenInHeader
    }
    ()
  }

  private def commitAsJson(commitId: CommitId, theMostRecentEventDate: Instant) = json"""{
    "id":              ${commitId.value},
    "author_name":     ${nonEmptyStrings().generateOne},
    "author_email":    ${userEmails.generateOne.value},
    "committer_name":  ${nonEmptyStrings().generateOne},
    "committer_email": ${userEmails.generateOne.value},
    "message":         ${nonEmptyStrings().generateOne},
    "committed_date":  ${theMostRecentEventDate.toString},
    "parent_ids":      []
  }  
  """

  def `GET <gitlabApi>/projects/:id/repository/commits/:sha returning OK with some event`(
      projectId:              Id,
      commitId:               CommitId,
      parentIds:              Set[CommitId] = Set.empty,
      theMostRecentEventDate: Instant = Instant.now()
  )(implicit accessToken:     AccessToken): StubMapping = stubFor {
    get(s"/api/v4/projects/$projectId/repository/commits/$commitId").withAccessTokenInHeader
      .willReturn(okJson(json"""{
          "id":              ${commitId.value},
          "author_name":     ${nonEmptyStrings().generateOne},
          "author_email":    ${userEmails.generateOne.value},
          "committer_name":  ${nonEmptyStrings().generateOne},
          "committer_email": ${userEmails.generateOne.value},
          "message":         ${nonEmptyStrings().generateOne},
          "committed_date":  ${theMostRecentEventDate.toString},
          "parent_ids":      ${parentIds.map(_.value).toList}
        }""".noSpaces))
  }

  def `GET <gitlabApi>/projects/:path/members returning OK with the list of members`(
      project:            data.Project
  )(implicit accessToken: AccessToken): Unit = {
    implicit val personEncoder: Encoder[Person] = Encoder.instance { person =>
      json"""{
          "id":       ${person.maybeGitLabId.map(_.value)},
          "username": ${person.name.value},
          "name":     ${person.name.value}
        }"""
    }

    stubFor {
      get(s"/api/v4/projects/${urlEncode(project.path.value)}/members").withAccessTokenInHeader
        .willReturn(okJson(project.entitiesProject.members.toList.asJson.noSpaces))
    }
    stubFor {
      get(s"/api/v4/projects/${urlEncode(project.path.value)}/users").withAccessTokenInHeader
        .willReturn(okJson(project.entitiesProject.members.toList.asJson.noSpaces))
    }
    ()
  }

  def `GET <gitlabApi>/projects/:path AND :id returning OK with`(
      project:            data.Project
  )(implicit accessToken: AccessToken): Unit = {

    implicit class PermissionsOps(permissions: Permissions) {
      lazy val toJson: Json = permissions match {
        case ProjectAndGroupPermissions(project, group) => json"""{
          "project_access": ${toJson(project)},
          "group_access":   ${toJson(group)}
        }"""
        case ProjectPermissions(project) => json"""{
          "project_access": ${toJson(project)},
          "group_access":   ${Json.Null}
        }"""
        case GroupPermissions(group) => json"""{
          "project_access": ${Json.Null},
          "group_access":   ${toJson(group)}
        }"""
      }

      private def toJson(accessLevel: AccessLevel): Json = json"""{
        "access_level": ${accessLevel.value.value}
      }"""
    }

    val returnedJson = okJson(
      json"""{
      "id":                   ${project.id.value},
      "name":                 ${project.name.value},
      "description":          ${project.maybeDescription.map(_.value)},
      "visibility":           ${project.entitiesProject.visibility.value},
      "path_with_namespace":  ${project.path.value},
      "ssh_url_to_repo":      ${project.urls.ssh.value},
      "http_url_to_repo":     ${project.urls.http.value},
      "web_url":              ${project.urls.web.value},
      "readme_url":           ${project.urls.maybeReadme.map(_.value)},
      "forks_count":          ${project.entitiesProject.forksCount.value},
      "tag_list":             ${project.tags.map(_.value).toList},
      "star_count":           ${project.starsCount.value},
      "created_at":           ${project.entitiesProject.dateCreated.value},
      "creator_id":           ${project.entitiesProject.maybeCreator.flatMap(_.maybeGitLabId.map(_.value))},
      "last_activity_at":     ${project.updatedAt.value},
      "permissions":          ${project.permissions.toJson},
      "statistics": {
        "commit_count":       ${project.statistics.commitsCount.value},
        "storage_size":       ${project.statistics.storageSize.value},
        "repository_size":    ${project.statistics.repositorySize.value},
        "lfs_objects_size":   ${project.statistics.lsfObjectsSize.value},
        "job_artifacts_size": ${project.statistics.jobArtifactsSize.value}
      }
    }""".deepMerge(
        project.entitiesProject match {
          case withParent: ProjectWithParent =>
            json"""{"forked_from_project":  {"path_with_namespace": ${withParent.parent.path.value}} }"""
          case _ => Json.obj()
        }
      ).deepMerge(
        project.entitiesProject.maybeCreator
          .flatMap(_.maybeGitLabId)
          .map(creatorId => json"""{"creator_id": ${creatorId.value}}""")
          .getOrElse(Json.obj())
      ).noSpaces
    )

    stubFor {
      get(urlPathEqualTo(s"/api/v4/projects/${urlEncode(project.path.value)}")).withAccessTokenInHeader
        .willReturn(returnedJson)
    }

    stubFor {
      get(urlPathEqualTo(s"/api/v4/projects/${project.id.value}")).withAccessTokenInHeader
        .willReturn(returnedJson)
    }

    (project.entitiesProject.maybeCreator -> project.entitiesProject.maybeCreator.flatMap(_.maybeGitLabId)) mapN {
      (creator, creatorId) =>
        stubFor {
          get(s"/api/v4/users/$creatorId").withAccessTokenInHeader
            .willReturn(
              okJson(json"""{
                "id":   ${creatorId.value},
                "username": ${creator.name.value},
                "name": ${creator.name.value}
              }""".noSpaces)
            )
        }
    }

    `GET <gitlabApi>/projects/:path/members returning OK with the list of members`(project)
  }

  def `GET <gitlabApi>/projects/:path returning BadRequest`(
      project:            data.Project
  )(implicit accessToken: AccessToken): StubMapping = stubFor {
    get(s"/api/v4/projects/${urlEncode(project.path.value)}").withAccessTokenInHeader
      .willReturn(badRequest())
  }

  def `GET <gitlabApi>/projects/:path having connectivity issues`(
      project:            data.Project
  )(implicit accessToken: AccessToken): StubMapping = stubFor {
    get(s"/api/v4/projects/${urlEncode(project.path.value)}").withAccessTokenInHeader
      .willReturn(aResponse() withFault CONNECTION_RESET_BY_PEER)
  }

  private implicit class MappingBuilderOps(builder: MappingBuilder) {
    def withAccessTokenInHeader(implicit accessToken: AccessToken): MappingBuilder = accessToken match {
      case PersonalAccessToken(token) => builder.withHeader("PRIVATE-TOKEN", equalTo(token))
      case OAuthAccessToken(token)    => builder.withHeader("Authorization", equalTo(s"Bearer $token"))
    }
  }

  private def stubFor(mappingBuilder: MappingBuilder): StubMapping = instance.register(mappingBuilder)
}

private object GitLabWiremockInstance {
  private val logger = TestLogger()

  val port: Int Refined Positive = 2048

  val instance = WireMock.create().http().host("localhost").port(port.value).build()

  private val server = {
    val newServer = new WireMockServer(WireMockConfiguration.wireMockConfig().port(port.value))
    newServer.start()
    WireMock.configureFor(newServer.port())
    logger.info(s"GitLab stub started")
    newServer
  }

  def shutdownGitLab(): Unit = {
    server.stop()
    server.shutdownServer()
    logger.info(s"GitLab stub stopped")
    ()
  }
}
