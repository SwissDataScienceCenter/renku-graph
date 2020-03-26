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

import ch.datascience.dbeventlog.EventStatus
import ch.datascience.dbeventlog.EventStatus.New
import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.acceptancetests.db.EventLog
import ch.datascience.graph.acceptancetests.flows.AccessTokenPresence.givenAccessTokenPresentFor
import ch.datascience.graph.acceptancetests.stubs.GitLab._
import ch.datascience.graph.acceptancetests.stubs.RemoteTriplesGenerator._
import ch.datascience.graph.acceptancetests.testing.AcceptanceTestPatience
import ch.datascience.graph.acceptancetests.tooling.GraphServices
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.http.client.AccessToken
import ch.datascience.knowledgegraph.projects.ProjectsGenerators.projects
import ch.datascience.webhookservice.model.HookToken
import io.circe.literal._
import org.http4s.Status._
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually
import org.scalatest.{FeatureSpec, GivenWhenThen}

class PushEventsConsumptionSpec
    extends FeatureSpec
    with GivenWhenThen
    with GraphServices
    with Eventually
    with AcceptanceTestPatience {

  feature("A Push Event sent to the services generates Commit Events in the Event Log") {

    scenario("Push Event not being processed yet gets translated into Commit Events in the Event Log") {

      implicit val accessToken: AccessToken = accessTokens.generateOne
      val project   = projects.generateOne
      val projectId = project.id
      val commitId  = commitIds.generateOne

      Given("commit with the commit id matching Push Event's 'after' exists on the project in GitLab")
      `GET <gitlab>/api/v4/projects/:id/repository/commits/:sha returning OK with some event`(projectId, commitId)

      // making the triples generation be happy and not throwing exceptions to the logs
      `GET <triples-generator>/projects/:id/commits/:id returning OK with some triples`(project, commitId)

      And("project exists in GitLab")
      `GET <gitlab>/api/v4/projects/:path returning OK with`(project)

      And("access token is present")
      givenAccessTokenPresentFor(project)

      When("user does POST webhook-service/webhooks/events happens")
      val payload  = json"""{
        "after": ${commitId.value},
        "project": {
          "id":                  ${projectId.value},
          "path_with_namespace": ${project.path.value}
        }
      }"""
      val response = webhookServiceClient.POST("webhooks/events", HookToken(projectId), payload)

      Then("he should get ACCEPTED response back")
      response.status shouldBe Accepted

      And("there should be a relevant event added to the Event Log")
      eventually {
        EventLog.findEvents(projectId, status = New) shouldBe List(commitId)
      }

      // wait for the Event Log to be emptied
      eventually {
        EventLog.findEvents(projectId, status = New)                    shouldBe empty
        EventLog.findEvents(projectId, status = EventStatus.Processing) shouldBe empty
      }
    }
  }
}
