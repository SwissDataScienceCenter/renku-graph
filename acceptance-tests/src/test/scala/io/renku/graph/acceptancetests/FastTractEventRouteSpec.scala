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

import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.data.dataProjects
import io.renku.graph.acceptancetests.flows.TSProvisioning
import io.renku.graph.acceptancetests.stubs.gitlab.GitLabStubIOSyntax
import io.renku.graph.acceptancetests.testing.AcceptanceTestPatience
import io.renku.graph.acceptancetests.tooling.GraphServices
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.testentities.generators.EntitiesGenerators.{renkuProjectEntities, visibilityPublic}
import io.renku.http.server.security.model.AuthUser
import io.renku.webhookservice.model.HookToken
import org.http4s.Status.{Accepted, Ok}
import org.scalatest.GivenWhenThen
import org.scalatest.concurrent.Eventually
import org.scalatest.featurespec.AnyFeatureSpec

class FastTractEventRouteSpec
    extends AnyFeatureSpec
    with GivenWhenThen
    with GraphServices
    with TSProvisioning
    with Eventually
    with AcceptanceTestPatience
    with GitLabStubIOSyntax {

  Feature("Fast track route for events not in the TS") {

    Scenario("Project with no events in the TS") {

      val user: AuthUser = authUsers.generateOne
      val project  = dataProjects(renkuProjectEntities(visibilityPublic)).generateOne
      val commitId = commitIds.generateOne

      Given("commit with the commit id matching Push Event's 'after' exists on the project in GitLab")
      gitLabStub.addAuthenticated(user)
      gitLabStub.setupProject(project, commitId)

      And("Triples are failing on generation")
      `GET <triples-generator>/projects/:id/commits/:id fails non recoverably`(project, commitId)

      And("access token is present")
      givenAccessTokenPresentFor(project)(user.accessToken)

      When("a Push Event arrives")
      webhookServiceClient
        .POST("webhooks/events", HookToken(project.id), data.GitLab.pushEvent(project, commitId))
        .status shouldBe Accepted

      And("the fast track event was sent")
      `wait for the Fast Tract event`(project.id)

      Then("the project data should exist in the KG")
      eventually {
        val projectDetailsResponse = knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.path}")
        projectDetailsResponse.status                                        shouldBe Ok
        projectDetailsResponse.jsonBody.hcursor.downField("path").as[String] shouldBe project.path.show.asRight
      }
    }
  }
}
