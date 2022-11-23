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
import io.circe.syntax.EncoderOps
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.data.Project.Statistics.CommitsCount
import io.renku.graph.acceptancetests.data.dataProjects
import io.renku.graph.acceptancetests.tooling.{AcceptanceSpec, ApplicationServices}
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.http.client.AccessToken
import org.http4s.Status._

class WebhookValidationEndpointSpec extends AcceptanceSpec with ApplicationServices {

  Feature("Existence of a Graph Services hook can be validated") {

    Scenario("There's a Graph Services hook on a Public project in GitLab") {
      val project = dataProjects(renkuProjectEntities(visibilityPublic), CommitsCount.zero).generateOne
      val user    = authUsers.generateOne
      Given("api user is authenticated")
      gitLabStub.addAuthenticated(user)

      Given("project is setup in GitLab")
      gitLabStub.setupProject(project)

      When("user does POST webhook-service/projects/:id/webhooks/validation")
      val response = webhookServiceClient.POST(s"projects/${project.id}/webhooks/validation", user.accessToken)

      Then("he should get OK response back")
      response.status shouldBe Ok
    }

    Scenario("There's no Graph Services hook on a Public project in GitLab") {

      val project = dataProjects(renkuProjectEntities(visibilityPublic), CommitsCount.zero).generateOne
      val user    = authUsers.generateOne
      Given("api user is authenticated")
      gitLabStub.addAuthenticated(user)

      Given("project is present in GitLab but has no Graph Services hooks")
      gitLabStub.addProject(project)

      When("user does POST webhook-service/projects/:id/webhooks/validation")
      val response = webhookServiceClient.POST(s"projects/${project.id}/webhooks/validation", user.accessToken)

      Then("he should get NOT_FOUND response back")
      response.status shouldBe NotFound
    }

    Scenario("There's a Graph Services hook on a non-public project in GitLab") {

      val user = authUsers.generateOne
      val project = dataProjects(
        renkuProjectEntities(visibilityNonPublic).map(
          _.copy(maybeCreator = personEntities(user.id.some).generateOne.some)
        )
      ).generateOne

      Given("api user is authenticated")
      gitLabStub.addAuthenticated(user)

      Given("project is setup in GitLab")
      gitLabStub.setupProject(project)

      When("user does POST webhook-service/projects/:id/webhooks/validation")
      val response = webhookServiceClient.POST(s"projects/${project.id}/webhooks/validation", user.accessToken)

      Then("he should get OK response back")
      response.status shouldBe Ok

      And("a Project Access Token should created for the Project and added to the token repository")
      tokenRepositoryClient
        .GET(s"projects/${project.id}/tokens")
        .jsonBody shouldBe gitLabStub
        .query(_.projectAccessTokens(project.id))
        .unsafeRunSync()
        .asInstanceOf[AccessToken]
        .asJson

      And("when the hook get deleted from GitLab")
      gitLabStub.removeWebhook(project.id)

      And("user does POST webhook-service/projects/:id/webhooks/validation again")
      val afterDeletionResponse =
        webhookServiceClient.POST(s"projects/${project.id}/webhooks/validation", user.accessToken)

      Then("he should get NOT_FOUND response back")
      afterDeletionResponse.status shouldBe NotFound

      And("the Access Token should be removed from the token repository")
      tokenRepositoryClient.GET(s"projects/${project.id}/tokens").status shouldBe NotFound
    }
  }
}
