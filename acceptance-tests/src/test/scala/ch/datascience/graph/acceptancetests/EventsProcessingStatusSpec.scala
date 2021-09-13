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

import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.acceptancetests.data.Project.Statistics.CommitsCount
import ch.datascience.graph.acceptancetests.data._
import ch.datascience.graph.acceptancetests.flows.AccessTokenPresence.givenAccessTokenPresentFor
import ch.datascience.graph.acceptancetests.stubs.GitLab._
import ch.datascience.graph.acceptancetests.stubs.RemoteTriplesGenerator._
import ch.datascience.graph.acceptancetests.testing.AcceptanceTestPatience
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.acceptancetests.tooling.{GraphServices, ModelImplicits}
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.events.CommitId
import ch.datascience.http.client.AccessToken
import ch.datascience.webhookservice.model.HookToken
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import org.http4s.Status._
import org.scalatest.GivenWhenThen
import org.scalatest.concurrent.Eventually
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should

import java.time.Instant

class EventsProcessingStatusSpec
    extends AnyFeatureSpec
    with ModelImplicits
    with GivenWhenThen
    with GraphServices
    with Eventually
    with AcceptanceTestPatience
    with should.Matchers {

  private val numberOfEvents: Int Refined Positive = 5

  Feature("Status of events processing for a given project") {

    Scenario("As a user I would like to see progress of events processing for my project") {

      implicit val accessToken: AccessToken = accessTokens.generateOne
      val project = dataProjects(projectEntities(visibilityPublic), CommitsCount(numberOfEvents.value)).generateOne

      When("there's no webhook for a given project in GitLab")
      Then("the status endpoint should return NOT_FOUND")
      webhookServiceClient.GET(s"projects/${project.id}/events/status").status shouldBe NotFound

      When("there is a webhook but no events in the Event Log")
      givenHookValidationToHookExists(project)

      Then("the status endpoint should return OK with done = total = 0")
      val noEventsResponse = webhookServiceClient GET s"projects/${project.id}/events/status"
      noEventsResponse.status shouldBe Ok
      val noEventsResponseJson = noEventsResponse.bodyAsJson.hcursor
      noEventsResponseJson.downField("done").as[Int]        shouldBe Right(0)
      noEventsResponseJson.downField("total").as[Int]       shouldBe Right(0)
      noEventsResponseJson.downField("progress").as[Double] shouldBe a[Left[_, _]]

      When("there are events under processing")
      sendEventsForProcessing(project)

      Then("the status endpoint should return OK with some progress info")
      eventually {
        val response = webhookServiceClient GET s"projects/${project.id}/events/status"

        response.status shouldBe Ok

        val responseJson = response.bodyAsJson.hcursor
        val Right(done)  = responseJson.downField("done").as[Int]
        done                                            should be <= numberOfEvents.value
        responseJson.downField("total").as[Int]       shouldBe Right(numberOfEvents.value)
        responseJson.downField("progress").as[Double] shouldBe Right(100d)
      }
    }
  }

  private def givenHookValidationToHookExists(project: data.Project)(implicit accessToken: AccessToken): Unit = {
    `GET <gitlabApi>/projects/:path AND :id returning OK with`(project)

    givenAccessTokenPresentFor(project)

    `GET <gitlabApi>/projects/:id/hooks returning OK with the hook`(project.id)
  }

  private def sendEventsForProcessing(project: data.Project)(implicit accessToken: AccessToken) = {

    val allCommitIds = commitIds.generateNonEmptyList(minElements = numberOfEvents, maxElements = numberOfEvents).toList

    `GET <gitlabApi>/projects/:id/repository/commits per page returning OK with a commit`(project.id, allCommitIds: _*)

    val theMostRecentEventDate = Instant.now()
    allCommitIds.foldLeft(Option.empty[CommitId]) { (maybePreviousCommitId, commitId) =>
      // GitLab to return commit info about all the parent commits
      `GET <gitlabApi>/projects/:id/repository/commits/:sha returning OK with some event`(project.id,
                                                                                          commitId,
                                                                                          maybePreviousCommitId.toSet,
                                                                                          theMostRecentEventDate
      )

      // making the triples generation process happy and not throwing exceptions to the logs
      `GET <triples-generator>/projects/:id/commits/:id returning OK with some triples`(project, commitId)

      Some(commitId)
    }

    webhookServiceClient
      .POST("webhooks/events", HookToken(project.id), data.GitLab.pushEvent(project, allCommitIds.last))
      .status shouldBe Accepted
  }
}
