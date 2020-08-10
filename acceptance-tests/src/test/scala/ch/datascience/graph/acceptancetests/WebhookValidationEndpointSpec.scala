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

import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.acceptancetests.stubs.GitLab._
import ch.datascience.graph.acceptancetests.tooling.GraphServices
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.acceptancetests.tooling.TokenRepositoryClient._
import ch.datascience.graph.model.GraphModelGenerators.projectIds
import ch.datascience.graph.model.projects.Visibility.{Private, Public}
import ch.datascience.http.client.AccessToken
import org.http4s.Status._
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec

class WebhookValidationEndpointSpec extends AnyFeatureSpec with GivenWhenThen with GraphServices with should.Matchers {

  Feature("Existence of a Graph Services hook can be validated") {

    Scenario("There's a Graph Services hook on a Public project in GitLab") {

      val projectId = projectIds.generateOne
      implicit val accessToken: AccessToken = accessTokens.generateOne

      Given("project is present in GitLab")
      `GET <gitlab>/api/v4/projects/:id returning OK`(projectId, projectVisibility = Public)

      Given("project has Graph Services hook in GitLab")
      `GET <gitlab>/api/v4/projects/:id/hooks returning OK with the hook`(projectId)

      When("user does POST webhook-service/projects/:id/webhooks/validation")
      val response = webhookServiceClient.POST(s"projects/$projectId/webhooks/validation", Some(accessToken))

      Then("he should get OK response back")
      response.status shouldBe Ok
    }

    scenario("There's no Graph Services hook on a Public project in GitLab") {

      val projectId = projectIds.generateOne
      implicit val accessToken: AccessToken = accessTokens.generateOne

      Given("project is present in GitLab")
      `GET <gitlab>/api/v4/projects/:id returning OK`(projectId, projectVisibility = Public)

      Given("project does not have Graph Services hook in GitLab")
      `GET <gitlab>/api/v4/projects/:id/hooks returning OK with no hooks`(projectId)

      When("user does POST webhook-service/projects/:id/webhooks/validation")
      val response = webhookServiceClient.POST(s"projects/$projectId/webhooks/validation", Some(accessToken))

      Then("he should get NOT_FOUND response back")
      response.status shouldBe NotFound
    }

    scenario("There's a Graph Services hook on a non-public project in GitLab") {

      val projectId = projectIds.generateOne
      implicit val accessToken: AccessToken = accessTokens.generateOne

      Given("project is present in GitLab")
      `GET <gitlab>/api/v4/projects/:id returning OK`(projectId, projectVisibility = Private)

      Given("project has Graph Services hook in GitLab")
      `GET <gitlab>/api/v4/projects/:id/hooks returning OK with the hook`(projectId)

      When("user does POST webhook-service/projects/:id/webhooks/validation")
      val response = webhookServiceClient.POST(s"projects/$projectId/webhooks/validation", Some(accessToken))

      Then("he should get OK response back")
      response.status shouldBe Ok

      And("the Access Token used in the POST should be added to the token repository")
      tokenRepositoryClient
        .GET(s"projects/$projectId/tokens")
        .bodyAsJson shouldBe accessToken.toJson

      And("when the hook get deleted from GitLab")
      `GET <gitlab>/api/v4/projects/:id/hooks returning OK with no hooks`(projectId)

      And("user does POST webhook-service/projects/:id/webhooks/validation again")
      val afterDeletionResponse =
        webhookServiceClient.POST(s"projects/$projectId/webhooks/validation", Some(accessToken))

      Then("he should get NOT_FOUND response back")
      afterDeletionResponse.status shouldBe NotFound

      And("the Access Token should be removed from the token repository")
      tokenRepositoryClient.GET(s"projects/$projectId/tokens").status shouldBe NotFound
    }
  }
}
