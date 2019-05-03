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

import ch.datascience.dbeventlog.EventStatus.{New, NonRecoverableFailure}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.acceptancetests.db.EventLog
import ch.datascience.graph.acceptancetests.stubs.GitLab._
import ch.datascience.graph.acceptancetests.tooling.GraphServices
import ch.datascience.graph.model.events.EventsGenerators.{commitIds, projectIds, projectPaths}
import ch.datascience.webhookservice.model.HookToken
import org.http4s.Status._
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.{FeatureSpec, GivenWhenThen}

class EventLogEventsHandlingSpec
    extends FeatureSpec
    with GivenWhenThen
    with GraphServices
    with Eventually
    with IntegrationPatience {

  feature("Commit Events from the Event Log to be translated to triples in a RDF Store") {

    scenario("Not processed Commit Events in the Event Log should be picked-up for processing") {

      val projectId   = projectIds.generateOne
      val projectPath = projectPaths.generateOne
      val commitId    = commitIds.generateOne

      Given("project having commit with the commit id in GitLab")
      `GET <gitlab>/api/v4/projects/:id/repository/commits/:sha returning OK with some event`(projectId, commitId)

      When("Push Event arrives")
      webhookServiceClient
        .POST("webhooks/events", HookToken(projectId), model.GitLab.pushEvent(projectId, projectPath, commitId))
        .status shouldBe Accepted

      Then("there should be an Commit Event added to the Event Log")
      EventLog.findEvents(projectId, status = New) shouldBe List(commitId)

      And("then the Event should be picked up by the Triples Generator and RDF triples should appear in the RDF store")
      eventually {
        // Unfortunately we cannot do any better as in order to store triples in the RDF Store
        // triples-generator needs to connect to a real GitLab and be able to run `renku log`
        EventLog.findEvents(projectId, status = NonRecoverableFailure) shouldBe List(commitId)
      }
    }
  }
}
