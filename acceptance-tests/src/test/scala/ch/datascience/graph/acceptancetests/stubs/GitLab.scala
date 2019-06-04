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

package ch.datascience.graph.acceptancetests.stubs

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.acceptancetests.tooling.GraphServices.webhookServiceClient
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.graph.model.events.{CommitId, ProjectId}
import ch.datascience.webhookservice.project.ProjectVisibility
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.literal._

object GitLab {

  def `GET <gitlab>/api/v4/projects/:id returning OK`(projectId:         ProjectId,
                                                      projectVisibility: ProjectVisibility): Unit = {
    stubFor {
      get(s"/api/v4/projects/$projectId")
        .willReturn(okJson(json"""
          {
            "id":                  ${projectId.value}, 
            "visibility":          ${projectVisibility.value}, 
            "path_with_namespace": ${relativePaths(minSegments = 2, maxSegments = 2).generateOne}
          }""".noSpaces))
    }
    ()
  }

  def `GET <gitlab>/api/v4/projects/:id/hooks returning OK with the hook`(projectId: ProjectId): Unit = {
    val webhookUrl = s"${webhookServiceClient.baseUrl}/webhooks/events"
    stubFor {
      get(s"/api/v4/projects/$projectId/hooks")
        .willReturn(okJson(json"""[{"url": $webhookUrl}]""".noSpaces))
    }
    ()
  }

  def `GET <gitlab>/api/v4/projects/:id/hooks returning OK with no hooks`(projectId: ProjectId): Unit = {
    stubFor {
      get(s"/api/v4/projects/$projectId/hooks")
        .willReturn(okJson(json"""[]""".noSpaces))
    }
    ()
  }

  def `POST <gitlab>/api/v4/projects/:id/hooks returning CREATED`(projectId: ProjectId): Unit = {
    stubFor {
      post(s"/api/v4/projects/$projectId/hooks")
        .willReturn(created())
    }
    ()
  }

  def `GET <gitlab>/api/v4/projects/:id/repository/commits returning OK with a commit`(projectId: ProjectId): Unit = {
    stubFor {
      get(s"/api/v4/projects/$projectId/repository/commits?per_page=1")
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
      projectId: ProjectId,
      commitId:  CommitId): Unit = {
    stubFor {
      get(s"/api/v4/projects/$projectId/repository/commits/$commitId")
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
}
