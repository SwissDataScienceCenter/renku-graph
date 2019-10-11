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

import ch.datascience.dbeventlog.EventStatus.New
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.acceptancetests.db.EventLog
import ch.datascience.graph.acceptancetests.stubs.GitLab._
import ch.datascience.graph.acceptancetests.testing.AcceptanceTestPatience
import ch.datascience.graph.acceptancetests.tooling.GraphServices
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
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

      GraphServices.restart(triplesGenerator)

      val projectId = projectIds.generateOne
      val commitId  = commitIds.generateOne

      Given("commit with the commit id matching Push Event's 'after' exists on the project in GitLab")
      `GET <gitlab>/api/v4/projects/:id/repository/commits/:sha returning OK with some event`(projectId, commitId)

      When("user does POST webhook-service/webhooks/events happens")
      val payload  = json"""
        {
          "after": ${commitId.value},
          "project": {
            "id":                  ${projectId.value},
            "path_with_namespace": ${projectPaths.generateOne.value}
          }
        }"""
      val response = webhookServiceClient.POST("webhooks/events", HookToken(projectId), payload)

      Then("he should get ACCEPTED response back")
      response.status shouldBe Accepted

      And("there should be a relevant event added to the Event Log")
      eventually {
        EventLog.findEvents(projectId, status = New) shouldBe List(commitId)
      }
    }
  }
}
