/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

import cats.syntax.all._
import data.{addMemberWithId, dataProjects}
import flows.TSProvisioning
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.projects.Role
import io.renku.graph.model.testentities.{cliShapedPersons, removeMembers}
import io.renku.graph.model.testentities.generators.EntitiesGenerators.{renkuProjectEntities, visibilityPublic}
import io.renku.webhookservice.model.HookToken
import org.http4s.Status.{Accepted, Ok}
import org.scalatest.concurrent.Eventually
import org.scalatest.EitherValues
import testing.AcceptanceTestPatience
import tooling.{AcceptanceSpec, ApplicationServices}

class FastTractEventRouteSpec
    extends AcceptanceSpec
    with ApplicationServices
    with TSProvisioning
    with Eventually
    with AcceptanceTestPatience
    with EitherValues {

  Feature("Fast track route for events not in the TS") {

    Scenario("Project with no events in the TS") {

      val user = authUsers.generateOne
      val project = dataProjects(
        renkuProjectEntities(visibilityPublic, creatorGen = cliShapedPersons).modify(removeMembers())
      ).map(addMemberWithId(user.id, Role.Owner)).generateOne

      Given("commit with the commit id matching Push Event's 'after' exists on the project in GitLab")
      gitLabStub.addAuthenticated(user)

      val commitId = commitIds.generateOne
      gitLabStub.setupProject(project, commitId)

      And("Triples are failing on generation")
      `GET <triples-generator>/projects/:id/commits/:id fails non recoverably`(project, commitId)

      And("access token is present")
      givenAccessTokenPresentFor(project, user.accessToken)

      When("a Push Event arrives")
      webhookServiceClient
        .POST("webhooks/events", HookToken(project.id), data.GitLab.pushEvent(project, commitId))
        .status shouldBe Accepted

      And("the fast track event was sent")
      `wait for the Fast Tract event`(project.id)

      Then("the project data should exist in the KG")
      eventually {
        val response = knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.slug}")
        response.status                                              shouldBe Ok
        response.jsonBody.hcursor.downField("path").as[String].value shouldBe project.slug.show
      }
    }
  }
}
