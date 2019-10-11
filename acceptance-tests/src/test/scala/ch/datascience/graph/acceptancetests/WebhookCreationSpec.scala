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

import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.acceptancetests.stubs.GitLab._
import ch.datascience.graph.acceptancetests.tooling.GraphServices
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.model.EventsGenerators.projectIds
import ch.datascience.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import ch.datascience.webhookservice.project.ProjectVisibility.Public
import io.circe.literal._
import org.http4s.Status._
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

class WebhookCreationSpec extends FeatureSpec with GivenWhenThen with GraphServices {

  feature("A Graph Services hook can be created for a project") {

    scenario("Graph Services hook is present on the project in GitLab") {

      val projectId   = projectIds.generateOne
      val accessToken = accessTokens.generateOne

      Given("project is present in GitLab")
      `GET <gitlab>/api/v4/projects/:id returning OK`(projectId, projectVisibility = Public)

      Given("project has Graph Services hook in GitLab")
      `GET <gitlab>/api/v4/projects/:id/hooks returning OK with the hook`(projectId)

      When("user does POST webhook-service/projects/:id/webhooks")
      val response = webhookServiceClient.POST(s"projects/$projectId/webhooks", Some(accessToken))

      Then("he should get OK response back")
      response.status shouldBe Ok
    }

    scenario("No Graph Services webhook on the project in GitLab") {

      val projectId   = projectIds.generateOne
      val accessToken = accessTokens.generateOne

      Given("project is present in GitLab")
      `GET <gitlab>/api/v4/projects/:id returning OK`(projectId, projectVisibility = Public)

      Given("project does not have Graph Services hook in GitLab")
      `GET <gitlab>/api/v4/projects/:id/hooks returning OK with no hooks`(projectId)

      Given("project creation in GitLab returning CREATED")
      `POST <gitlab>/api/v4/projects/:id/hooks returning CREATED`(projectId)

      Given("some Commit for the project exists in GitLab")
      `GET <gitlab>/api/v4/projects/:id/repository/commits returning OK with a commit`(projectId)

      When("user does POST webhook-service/projects/:id/webhooks")
      val response = webhookServiceClient.POST(s"projects/$projectId/webhooks", Some(accessToken))

      Then("he should get CREATED response back")
      response.status shouldBe Created

      And("the Access Token used in the POST should be added to the token repository")
      val expectedAccessTokenJson = accessToken match {
        case OAuthAccessToken(token)    => json"""{"oauthAccessToken": $token}"""
        case PersonalAccessToken(token) => json"""{"personalAccessToken": $token}"""
      }
      tokenRepositoryClient
        .GET(s"projects/$projectId/tokens")
        .bodyAsJson shouldBe expectedAccessTokenJson
    }
  }
}
