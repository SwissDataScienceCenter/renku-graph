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

package ch.datascience.graph.acceptancetests

import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.acceptancetests.db.EventLog
import ch.datascience.graph.acceptancetests.flows.AccessTokenPresence.givenAccessTokenPresentFor
import ch.datascience.graph.acceptancetests.stubs.GitLab._
import ch.datascience.graph.acceptancetests.stubs.RemoteTriplesGenerator._
import ch.datascience.graph.acceptancetests.testing.AcceptanceTestPatience
import ch.datascience.graph.acceptancetests.tooling.GraphServices
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.acceptancetests.tooling.TokenRepositoryClient._
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.projects.Visibility.Public
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.http.client.AccessToken
import ch.datascience.knowledgegraph.projects.ProjectsGenerators._
import ch.datascience.knowledgegraph.projects.model.Project
import ch.datascience.webhookservice.model.HookToken
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.eventlog.EventStatus.New
import org.http4s.Status._
import org.scalatest.GivenWhenThen
import org.scalatest.concurrent.Eventually
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers._

import scala.language.postfixOps

class EventsProcessingStatusSpec
    extends AnyFeatureSpec
    with GivenWhenThen
    with GraphServices
    with Eventually
    with AcceptanceTestPatience
    with should.Matchers {

  private val numberOfEvents: Int Refined Positive = 5

  Feature("Status of events processing for a given project") {

    Scenario("As a user I would like to see progress of events processing for my project") {

      val project   = projects.generateOne
      val projectId = project.id
      implicit val accessToken: AccessToken = accessTokens.generateOne

      When("there's no webhook for a given project in GitLab")
      Then("the status endpoint should return NOT_FOUND")
      webhookServiceClient.GET(s"projects/$projectId/events/status").status shouldBe NotFound

      When("there is a webhook but no events in the Event Log")
      givenHookValidationToHookExists(projectId, project.path)

      Then("the status endpoint should return OK with done = total = 0")
      val noEventsResponse = webhookServiceClient GET s"projects/$projectId/events/status"
      noEventsResponse.status shouldBe Ok
      val noEventsResponseJson = noEventsResponse.bodyAsJson.hcursor
      noEventsResponseJson.downField("done").as[Int]        shouldBe Right(0)
      noEventsResponseJson.downField("total").as[Int]       shouldBe Right(0)
      noEventsResponseJson.downField("progress").as[Double] shouldBe a[Left[_, _]]

      When("there are events under processing")
      sendEventsForProcessing(project)

      Then("the status endpoint should return OK with some progress info")
      eventually {
        val response = webhookServiceClient GET s"projects/$projectId/events/status"

        response.status shouldBe Ok

        val responseJson = response.bodyAsJson.hcursor
        val Right(done)  = responseJson.downField("done").as[Int]
        done should be <= numberOfEvents.value
        val Right(total) = responseJson.downField("total").as[Int]
        total shouldBe numberOfEvents.value
        val Right(progress) = responseJson.downField("progress").as[Double]
        progress should be <= 100d
      }

      // wait for the Event Log to be emptied
      eventually {
        EventLog.findEvents(projectId, status = New) shouldBe List.empty
      }
    }
  }

  private def givenHookValidationToHookExists(
      projectId:          Id,
      projectPath:        Path
  )(implicit accessToken: AccessToken): Unit = {
    `GET <gitlab>/api/v4/projects/:id returning OK with Project Path`(projectId, projectPath)
    tokenRepositoryClient
      .PUT(s"projects/$projectId/tokens", accessToken.toJson, maybeAccessToken = None)
      .status shouldBe NoContent
    `GET <gitlab>/api/v4/projects/:id returning OK`(projectId, projectVisibility = Public)
    `GET <gitlab>/api/v4/projects/:id/hooks returning OK with the hook`(projectId)
  }

  private def sendEventsForProcessing(project: Project)(implicit accessToken: AccessToken) = {
    val allCommitIds = nonEmptyList(
      commitIds,
      minElements = numberOfEvents,
      maxElements = numberOfEvents
    ).generateOne

    givenAccessTokenPresentFor(project)

    `GET <gitlab>/api/v4/projects/:id/repository/commits/:sha returning OK with some event`(project.id,
                                                                                            allCommitIds.head,
                                                                                            allCommitIds.tail.toSet)
    // assuring there's project info in GitLab for the triples curation process
    `GET <gitlab>/api/v4/projects/:path returning OK with`(project)

    allCommitIds.map { commitId =>
      // GitLab to return commit info about all the parent commits
      if (commitId != allCommitIds.head)
        `GET <gitlab>/api/v4/projects/:id/repository/commits/:sha returning OK with some event`(project.id, commitId)

      // making the triples generation be happy and not throwing exceptions to the logs
      `GET <triples-generator>/projects/:id/commits/:id returning OK with some triples`(project, commitId)
    }

    webhookServiceClient
      .POST("webhooks/events", HookToken(project.id), data.GitLab.pushEvent(project, allCommitIds.head))
      .status shouldBe Accepted
  }
}
