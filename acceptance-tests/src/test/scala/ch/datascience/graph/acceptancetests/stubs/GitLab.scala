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

package ch.datascience.graph.acceptancetests.stubs

import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.acceptancetests.tooling.GraphServices.webhookServiceClient
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.events.{CommitId, Project}
import ch.datascience.graph.model.projects.{Id, Path, Visibility}
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import ch.datascience.http.client.UrlEncoder.urlEncode
import ch.datascience.knowledgegraph.projects.model.{ParentProject, Project => ProjectMetadata}
import com.github.tomakehurst.wiremock.client.MappingBuilder
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.Json
import io.circe.literal._

object GitLab {

  def `GET <gitlab>/api/v4/projects/:id returning OK`(
      projectId:          Id,
      projectVisibility:  Visibility
  )(implicit accessToken: AccessToken): Unit = {
    stubFor {
      get(s"/api/v4/projects/$projectId").withAccessTokenInHeader
        .willReturn(okJson(json"""
          {
            "id":                  ${projectId.value}, 
            "visibility":          ${projectVisibility.value}, 
            "path_with_namespace": ${relativePaths(minSegments = 2, maxSegments = 2).generateOne}
          }""".noSpaces))
    }
    ()
  }

  def `GET <gitlab>/api/v4/projects/:id/hooks returning OK with the hook`(
      projectId:          Id
  )(implicit accessToken: AccessToken): Unit = {
    val webhookUrl = s"${webhookServiceClient.baseUrl}/webhooks/events"
    stubFor {
      get(s"/api/v4/projects/$projectId/hooks").withAccessTokenInHeader
        .willReturn(okJson(json"""[{"url": $webhookUrl}]""".noSpaces))
    }
    ()
  }

  def `GET <gitlab>/api/v4/projects/:id/hooks returning OK with no hooks`(
      projectId:          Id
  )(implicit accessToken: AccessToken): Unit = {
    stubFor {
      get(s"/api/v4/projects/$projectId/hooks").withAccessTokenInHeader
        .willReturn(okJson(json"""[]""".noSpaces))
    }
    ()
  }

  def `POST <gitlab>/api/v4/projects/:id/hooks returning CREATED`(
      projectId:          Id
  )(implicit accessToken: AccessToken): Unit = {
    stubFor {
      post(s"/api/v4/projects/$projectId/hooks").withAccessTokenInHeader
        .willReturn(created())
    }
    ()
  }

  def `GET <gitlab>/api/v4/projects/:id/repository/commits returning OK with a commit`(
      projectId:          Id
  )(implicit accessToken: AccessToken): Unit = {
    stubFor {
      get(s"/api/v4/projects/$projectId/repository/commits?per_page=1").withAccessTokenInHeader
        .willReturn(okJson(json"""[
          {
            "id":              ${commitIds.generateOne.value},
            "author_name":     ${nonEmptyStrings().generateOne},
            "author_email":    ${emails.generateOne.value},
            "committer_name":  ${nonEmptyStrings().generateOne},
            "committer_email": ${emails.generateOne.value},
            "message":         ${nonEmptyStrings().generateOne},
            "committed_date":  ${committedDates.generateOne.value.toString},
            "parent_ids":      []
          }                         
        ]""".noSpaces))
    }
    ()
  }

  def `GET <gitlab>/api/v4/projects/:id/repository/commits/:sha returning OK with some event`(
      projectId:          Id,
      commitId:           CommitId
  )(implicit accessToken: AccessToken): Unit = {
    stubFor {
      get(s"/api/v4/projects/$projectId/repository/commits/$commitId").withAccessTokenInHeader
        .willReturn(okJson(json"""
          {
            "id":              ${commitId.value},
            "author_name":     ${nonEmptyStrings().generateOne},
            "author_email":    ${emails.generateOne.value},
            "committer_name":  ${nonEmptyStrings().generateOne},
            "committer_email": ${emails.generateOne.value},
            "message":         ${nonEmptyStrings().generateOne},
            "committed_date":  ${committedDates.generateOne.value.toString},
            "parent_ids":      []
          }                         
        """.noSpaces))
    }
    ()
  }

  def `GET <gitlab>/api/v4/projects/:id returning OK with Project Path`(
      project:            Project
  )(implicit accessToken: AccessToken): Unit =
    `GET <gitlab>/api/v4/projects/:id returning OK with Project Path`(project.id, project.path)

  def `GET <gitlab>/api/v4/projects/:id returning OK with Project Path`(
      projectId:          Id,
      projectPath:        Path
  )(implicit accessToken: AccessToken): Unit = {
    stubFor {
      get(s"/api/v4/projects/$projectId").withAccessTokenInHeader
        .willReturn(okJson(json"""{
          "id":                  ${projectId.value},
          "path_with_namespace": ${projectPath.value}
        }""".noSpaces))
    }
    ()
  }

  def `GET <gitlab>/api/v4/projects/:path returning OK with`(
      project:            ProjectMetadata
  )(implicit accessToken: AccessToken): Unit = {

    def toJson(parent: ParentProject) = json"""{
      "id":                  ${parent.id.value},
      "path_with_namespace": ${parent.path.value},
      "name":                ${parent.name.value}
    }"""

    stubFor {
      get(s"/api/v4/projects/${urlEncode(project.path.value)}").withAccessTokenInHeader
        .willReturn(
          okJson(
            json"""{
              "id":               ${project.id.value},
              "description":      ${project.maybeDescription.map(_.value)},
              "visibility":       ${project.visibility.value},
              "ssh_url_to_repo":  ${project.urls.ssh.value},
              "http_url_to_repo": ${project.urls.http.value},
              "web_url":          ${project.urls.web.value},
              "readme_url":       ${project.urls.readme.value},
              "forks_count":      ${project.forking.forksCount.value},
              "star_count":       ${project.starsCount.value},
              "last_activity_at": ${project.updatedAt.value},
              "permissions": {
                "project_access": {
                  "access_level": ${project.permissions.projectAccessLevel.value.value}
                },
                "group_access": {
                  "access_level": ${project.permissions.groupAccessLevel.value.value}
                }
              }
            }"""
              .deepMerge(
                project.forking.maybeParent
                  .map(parent => Json.obj("forked_from_project" -> toJson(parent)))
                  .getOrElse(Json.obj())
              )
              .noSpaces
          )
        )
    }
    ()
  }

  private implicit class MappingBuilderOps(builder: MappingBuilder) {
    def withAccessTokenInHeader(implicit accessToken: AccessToken): MappingBuilder = accessToken match {
      case PersonalAccessToken(token) => builder.withHeader("PRIVATE-TOKEN", equalTo(token))
      case OAuthAccessToken(token)    => builder.withHeader("Authorization", equalTo(s"Bearer $token"))
    }
  }
}
