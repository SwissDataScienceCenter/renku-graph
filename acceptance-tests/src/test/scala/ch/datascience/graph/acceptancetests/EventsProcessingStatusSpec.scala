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

package ch.datascience.graph.acceptancetests

import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.acceptancetests.stubs.GitLab._
import ch.datascience.graph.acceptancetests.tooling.GraphServices
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.acceptancetests.tooling.TokenRepositoryClient._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.graph.model.events.ProjectId
import ch.datascience.webhookservice.model.HookToken
import ch.datascience.webhookservice.project.ProjectVisibility.Public
import org.http4s.Status._
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.{FeatureSpec, GivenWhenThen}

import scala.language.postfixOps

class EventsProcessingStatusSpec
    extends FeatureSpec
    with GivenWhenThen
    with GraphServices
    with Eventually
    with IntegrationPatience {

  private val numberOfEvents = 10

  feature("Status of events processing for a given project") {

    scenario("As a user I would like to see progress of events processing for my project") {

      val projectId = projectIds.generateOne

      When("there's no webhook for a given project in GitLab")
      Then("the status endpoint should return NOT_FOUND")
      webhookServiceClient.GET(s"projects/$projectId/events/status", maybeAccessToken = None).status shouldBe NotFound

      When("there is a webhook but no events in the Event Log")
      givenHookValidationToHookExists(projectId)

      Then("the status endpoint should return NOT_FOUND")
      webhookServiceClient.GET(s"projects/$projectId/events/status", maybeAccessToken = None).status shouldBe NotFound

      When("there are events being processed")
      sendEventsForProcessing(projectId)

      Then("the status endpoint should return OK with some progress info")
      eventually {
        val statusResult = webhookServiceClient.GET(s"projects/$projectId/events/status", maybeAccessToken = None)

        statusResult.status shouldBe Ok

        val response    = statusResult.bodyAsJson.hcursor
        val Right(done) = response.downField("done").as[Int]
        done should be <= numberOfEvents
        val Right(total) = response.downField("total").as[Int]
        total shouldBe numberOfEvents
        val Right(progress) = response.downField("progress").as[Double]
        progress should be <= 100D
      }
    }
  }

  private def givenHookValidationToHookExists(projectId: ProjectId): Unit = {
    tokenRepositoryClient
      .PUT(s"projects/$projectId/tokens", accessTokens.generateOne.toJson, maybeAccessToken = None)
      .status shouldBe NoContent
    `GET <gitlab>/api/v4/projects/:id returning OK`(projectId, projectVisibility = Public)
    `GET <gitlab>/api/v4/projects/:id/hooks returning OK with the hook`(projectId)
  }

  private def sendEventsForProcessing(projectId: ProjectId): Unit =
    nonEmptyList(commitIds, minElements = numberOfEvents, maxElements = numberOfEvents).generateOne
      .map { commitId =>
        `GET <gitlab>/api/v4/projects/:id/repository/commits/:sha returning OK with some event`(projectId, commitId)

        webhookServiceClient
          .POST("webhooks/events", HookToken(projectId), model.GitLab.pushEvent(projectId, commitId))
          .status shouldBe Accepted
      }
}
