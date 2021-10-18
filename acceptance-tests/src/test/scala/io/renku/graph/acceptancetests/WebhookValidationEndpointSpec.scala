/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

import io.circe.syntax.EncoderOps
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.data.Project.Statistics.CommitsCount
import io.renku.graph.acceptancetests.data.dataProjects
import io.renku.graph.acceptancetests.stubs.GitLab._
import io.renku.graph.acceptancetests.tooling.GraphServices
import io.renku.graph.acceptancetests.tooling.ResponseTools._
import io.renku.http.client.AccessToken
import org.http4s.Status._
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should

class WebhookValidationEndpointSpec extends AnyFeatureSpec with GivenWhenThen with GraphServices with should.Matchers {

  Feature("Existence of a Graph Services hook can be validated") {

    Scenario("There's a Graph Services hook on a Public project in GitLab") {
      val project = dataProjects(projectEntities(visibilityPublic), CommitsCount.zero).generateOne
      implicit val accessToken: AccessToken = accessTokens.generateOne
      Given("api user is authenticated")
      `GET <gitlabApi>/user returning OK`()

      Given("project is present in GitLab")
      `GET <gitlabApi>/projects/:path AND :id returning OK with`(project)

      Given("project has Graph Services hook in GitLab")
      `GET <gitlabApi>/projects/:id/hooks returning OK with the hook`(project.id)

      When("user does POST webhook-service/projects/:id/webhooks/validation")
      val response = webhookServiceClient.POST(s"projects/${project.id}/webhooks/validation", Some(accessToken))

      Then("he should get OK response back")
      response.status shouldBe Ok
    }

    Scenario("There's no Graph Services hook on a Public project in GitLab") {

      val project = dataProjects(projectEntities(visibilityPublic), CommitsCount.zero).generateOne
      implicit val accessToken: AccessToken = accessTokens.generateOne
      Given("api user is authenticated")
      `GET <gitlabApi>/user returning OK`()

      Given("project is present in GitLab")
      `GET <gitlabApi>/projects/:path AND :id returning OK with`(project)

      Given("project does not have Graph Services hook in GitLab")
      `GET <gitlabApi>/projects/:id/hooks returning OK with no hooks`(project.id)

      When("user does POST webhook-service/projects/:id/webhooks/validation")
      val response = webhookServiceClient.POST(s"projects/${project.id}/webhooks/validation", Some(accessToken))

      Then("he should get NOT_FOUND response back")
      response.status shouldBe NotFound
    }

    Scenario("There's a Graph Services hook on a non-public project in GitLab") {

      val project = dataProjects(projectEntities(visibilityNonPublic)).generateOne
      implicit val accessToken: AccessToken = accessTokens.generateOne
      Given("api user is authenticated")
      `GET <gitlabApi>/user returning OK`()

      Given("project is present in GitLab")
      `GET <gitlabApi>/projects/:path AND :id returning OK with`(project)

      Given("project has Graph Services hook in GitLab")
      `GET <gitlabApi>/projects/:id/hooks returning OK with the hook`(project.id)

      When("user does POST webhook-service/projects/:id/webhooks/validation")
      val response = webhookServiceClient.POST(s"projects/${project.id}/webhooks/validation", Some(accessToken))

      Then("he should get OK response back")
      response.status shouldBe Ok

      And("the Access Token used in the POST should be added to the token repository")
      tokenRepositoryClient
        .GET(s"projects/${project.id}/tokens")
        .bodyAsJson shouldBe accessToken.asJson

      And("when the hook get deleted from GitLab")
      `GET <gitlabApi>/projects/:id/hooks returning OK with no hooks`(project.id)

      And("user does POST webhook-service/projects/:id/webhooks/validation again")
      val afterDeletionResponse =
        webhookServiceClient.POST(s"projects/${project.id}/webhooks/validation", Some(accessToken))

      Then("he should get NOT_FOUND response back")
      afterDeletionResponse.status shouldBe NotFound

      And("the Access Token should be removed from the token repository")
      tokenRepositoryClient.GET(s"projects/${project.id}/tokens").status shouldBe NotFound
    }
  }
}
