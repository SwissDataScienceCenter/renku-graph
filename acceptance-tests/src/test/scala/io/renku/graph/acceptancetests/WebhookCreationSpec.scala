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
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.data.Project.Statistics.CommitsCount
import io.renku.graph.acceptancetests.data.dataProjects
import io.renku.graph.acceptancetests.flows.AccessTokenPresence
import io.renku.graph.acceptancetests.tooling.{GraphServices, ModelImplicits}
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.http.client.AccessToken
import org.http4s.Status._
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should

class WebhookCreationSpec
    extends AnyFeatureSpec
    with ModelImplicits
    with GivenWhenThen
    with GraphServices
    with AccessTokenPresence
    with should.Matchers {

  Feature("A Graph Services hook can be created for a project") {

    Scenario("Graph Services hook is present on the project in GitLab") {

      val project = dataProjects(projectEntities(visibilityPublic), CommitsCount.zero).generateOne

      implicit val accessToken: AccessToken = accessTokens.generateOne
      Given("api user is authenticated")
      `GET <gitlabApi>/user returning OK`()

      Given("project is present in GitLab")
      `GET <gitlabApi>/projects/:path AND :id returning OK with`(project)

      Given("project has Graph Services hook in GitLab")
      `GET <gitlabApi>/projects/:id/hooks returning OK with the hook`(project.id)

      When("user does POST webhook-service/projects/:id/webhooks")
      val response = webhookServiceClient.POST(s"projects/${project.id}/webhooks", Some(accessToken))

      Then("he should get OK response back")
      response.status shouldBe Ok
    }

    Scenario("No Graph Services webhook on the project in GitLab") {

      implicit val accessToken: AccessToken = accessTokens.generateOne
      val project = dataProjects(projectEntities(visibilityPublic), CommitsCount.one).generateOne

      Given("api user is authenticated")
      `GET <gitlabApi>/user returning OK`()

      Given("project is present in GitLab")
      `GET <gitlabApi>/projects/:path AND :id returning OK with`(project)

      Given("project does not have Graph Services hook in GitLab")
      `GET <gitlabApi>/projects/:id/hooks returning OK with no hooks`(project.id)

      Given("project creation in GitLab returning CREATED")
      `POST <gitlabApi>/projects/:id/hooks returning CREATED`(project.id)

      Given("some Commit exists for the project in GitLab")
      givenAccessTokenPresentFor(project)
      val commitId = commitIds.generateOne
      `GET <gitlabApi>/projects/:id/repository/commits per page returning OK with a commit`(project.id, commitId)
      `GET <gitlabApi>/projects/:id/repository/commits/:sha returning OK with some event`(project.id, commitId)

      // making the triples generation be happy and not throwing exceptions to the logs
      `GET <triples-generator>/projects/:id/commits/:id returning OK with some triples`(project, commitId)

      When("user does POST webhook-service/projects/:id/webhooks")
      val response = webhookServiceClient.POST(s"projects/${project.id}/webhooks", Some(accessToken))

      Then("he should get CREATED response back")
      response.status shouldBe Created

      And("the Access Token used in the POST should be added to the token repository")

      val expectedAccessTokenJson = accessToken.asJson

      tokenRepositoryClient
        .GET(s"projects/${project.id}/tokens")
        .jsonBody shouldBe expectedAccessTokenJson
    }
  }
}
