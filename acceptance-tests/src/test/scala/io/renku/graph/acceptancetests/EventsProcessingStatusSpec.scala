/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.graph.acceptancetests

import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.data.Project.Statistics.CommitsCount
import io.renku.graph.acceptancetests.data._
import io.renku.graph.acceptancetests.flows.{AccessTokenPresence, RdfStoreProvisioning}
import io.renku.graph.acceptancetests.testing.AcceptanceTestPatience
import io.renku.graph.acceptancetests.tooling.{GraphServices, ModelImplicits}
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.http.client.AccessToken
import io.renku.jsonld.syntax._
import io.renku.webhookservice.model.HookToken
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
    with RdfStoreProvisioning
    with AccessTokenPresence
    with Eventually
    with AcceptanceTestPatience
    with should.Matchers {

  private val numberOfEvents: Int Refined Positive = 5

  Feature("Status of events processing for a given project") {

    Scenario("As a user I would like to see progress of events processing for my project") {

      implicit val accessToken: AccessToken = accessTokens.generateOne
      val project = dataProjects(renkuProjectEntities(visibilityPublic), CommitsCount(numberOfEvents.value)).generateOne

      When("there's no webhook for a given project in GitLab")
      Then("the status endpoint should return NOT_FOUND")
      webhookServiceClient.GET(s"projects/${project.id}/events/status").status shouldBe NotFound

      Given("there's an access token for the user")
      givenAccessTokenPresentFor(project)

      When("there is a webhook created")
      `GET <gitlabApi>/projects/:id/hooks returning OK with the hook`(project.id)

      And("there are events under processing")
      sendEventsForProcessing(project)

      Then("the status endpoint should return OK with some progress info")
      eventually {
        val response = webhookServiceClient GET s"projects/${project.id}/events/status"

        response.status shouldBe Ok

        val responseJson = response.jsonBody.hcursor
        val Right(done)  = responseJson.downField("done").as[Int]
        val Right(total) = responseJson.downField("total").as[Int]
        done                                            should (be <= numberOfEvents.value and be >= 1)
        total                                           should (be <= numberOfEvents.value and be >= 1)
        responseJson.downField("progress").as[Double] shouldBe Right(100d)
      }
    }
  }

  private def sendEventsForProcessing(project: data.Project)(implicit accessToken: AccessToken) = {

    val allCommitIds = commitIds.generateNonEmptyList(minElements = numberOfEvents, maxElements = numberOfEvents).toList

    `GET <gitlabApi>/projects/:id/repository/commits per page returning OK with a commit`(project.id, allCommitIds: _*)

    `data in the RDF store`(project, project.entitiesProject.asJsonLD, allCommitIds.head)

    val theMostRecentEventDate = Instant.now()
    allCommitIds.tail.foldLeft(allCommitIds.head) { (previousCommitId, commitId) =>
      // GitLab to return commit info about all the parent commits
      `GET <gitlabApi>/projects/:id/repository/commits/:sha returning OK with some event`(project,
                                                                                          commitId,
                                                                                          Set(previousCommitId),
                                                                                          theMostRecentEventDate
      )

      `GET <gitlabApi>/users/:id/events/?action=pushed&page=1 returning OK`(project.entitiesProject.maybeCreator,
                                                                            project,
                                                                            commitId
      )

      // making the triples generation process happy and not throwing exceptions to the logs
      `GET <triples-generator>/projects/:id/commits/:id returning OK with some triples`(project, commitId)

      commitId
    }

    webhookServiceClient
      .POST("webhooks/events", HookToken(project.id), data.GitLab.pushEvent(project, allCommitIds.last))
      .status shouldBe Accepted
  }
}
