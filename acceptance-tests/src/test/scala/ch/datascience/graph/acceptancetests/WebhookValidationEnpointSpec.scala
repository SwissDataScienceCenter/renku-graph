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

import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.acceptancetests.stubs.GitLab._
import ch.datascience.graph.acceptancetests.tooling.GraphServices
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import ch.datascience.webhookservice.project.ProjectVisibility.{Private, Public}
import io.circe.literal._
import org.http4s.Status._
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

class WebhookValidationEnpointSpec extends FeatureSpec with GivenWhenThen with GraphServices {

  feature(
    "The user can verify if there's a webhook for a project by doing POST webhook-service/projects/:id/webhooks/validation") {

    scenario("There's a Graph Services webhook on a Public project in GitLab") {

      val projectId   = projectIds.generateOne
      val accessToken = accessTokens.generateOne

      Given("project having info in GitLab")
      `GET <gitlab>/api/v4/projects/:id returning OK`(projectId, projectVisibility = Public)

      Given("project having Graph Services hook in GitLab")
      `GET <gitlab>/api/v4/projects/:id/hooks returning OK`(projectId)

      When("user does POST webhook-service/projects/:id/webhooks/validation")
      val response = webhookServiceClient.POST(s"projects/$projectId/webhooks/validation", Some(accessToken))

      Then("he should get OK response back")
      response.status shouldBe Ok
    }

    scenario("There's no Graph Services webhook on a Public project in GitLab") {

      val projectId   = projectIds.generateOne
      val accessToken = accessTokens.generateOne

      Given("project having info in GitLab")
      `GET <gitlab>/api/v4/projects/:id returning OK`(projectId, projectVisibility = Public)

      Given("project not having Graph Services hook in GitLab")
      `GET <gitlab>/api/v4/projects/:id/hooks returning OK with no hooks`(projectId)

      When("user does POST webhook-service/projects/:id/webhooks/validation")
      val response = webhookServiceClient.POST(s"projects/$projectId/webhooks/validation", Some(accessToken))

      Then("he should get NOT_FOUND response back")
      response.status shouldBe NotFound
    }

    scenario("There's a Graph Services webhook on a non-public project in GitLab") {

      val projectId   = projectIds.generateOne
      val accessToken = accessTokens.generateOne

      Given("project having info in GitLab")
      `GET <gitlab>/api/v4/projects/:id returning OK`(projectId, projectVisibility = Private)

      Given("project having Graph Services hook in GitLab")
      `GET <gitlab>/api/v4/projects/:id/hooks returning OK`(projectId)

      When("user does POST webhook-service/projects/:id/webhooks/validation")
      val response = webhookServiceClient.POST(s"projects/$projectId/webhooks/validation", Some(accessToken))

      Then("he should get OK response back")
      response.status shouldBe Ok

      And("the access token used in the POST should get associated with the project")
      val expectedAccessTokenJson = accessToken match {
        case OAuthAccessToken(token)    => json"""{"oauthAccessToken": $token}"""
        case PersonalAccessToken(token) => json"""{"personalAccessToken": $token}"""
      }
      tokenRepositoryClient
        .GET(s"projects/$projectId/tokens", maybeAccessToken = None)
        .bodyAsJson shouldBe expectedAccessTokenJson
    }
  }
}
