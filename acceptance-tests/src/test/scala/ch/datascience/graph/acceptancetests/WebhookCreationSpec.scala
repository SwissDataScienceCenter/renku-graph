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

package ch.datascience.graph.acceptancetests

import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.acceptancetests.data.dataProjects
import ch.datascience.graph.acceptancetests.flows.AccessTokenPresence.givenAccessTokenPresentFor
import ch.datascience.graph.acceptancetests.stubs.GitLab._
import ch.datascience.graph.acceptancetests.stubs.RemoteTriplesGenerator._
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.acceptancetests.tooling.{GraphServices, ModelImplicits}
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.testentities.EntitiesGenerators._
import ch.datascience.graph.model.testentities.Project.ForksCount
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import io.circe.literal._
import org.http4s.Status._
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should

class WebhookCreationSpec
    extends AnyFeatureSpec
    with ModelImplicits
    with GivenWhenThen
    with GraphServices
    with should.Matchers {

  Feature("A Graph Services hook can be created for a project") {

    Scenario("Graph Services hook is present on the project in GitLab") {

      val project = dataProjects(projectEntities[ForksCount.Zero](visibilityPublic)).generateOne

      implicit val accessToken: AccessToken = accessTokens.generateOne
      Given("api user is authenticated")
      `GET <gitlabApi>/user returning OK`()

      Given("project is present in GitLab")
      `GET <gitlabApi>/projects/:id returning OK`(project)

      Given("project has Graph Services hook in GitLab")
      `GET <gitlabApi>/projects/:id/hooks returning OK with the hook`(project.id)

      When("user does POST webhook-service/projects/:id/webhooks")
      val response = webhookServiceClient.POST(s"projects/${project.id}/webhooks", Some(accessToken))

      Then("he should get OK response back")
      response.status shouldBe Ok
    }

    Scenario("No Graph Services webhook on the project in GitLab") {

      val project   = dataProjects(projectEntities[ForksCount.Zero](visibilityPublic)).generateOne
      val projectId = project.id
      implicit val accessToken: AccessToken = accessTokens.generateOne
      Given("api user is authenticated")
      `GET <gitlabApi>/user returning OK`()

      Given("project is present in GitLab")
      `GET <gitlabApi>/projects/:id returning OK`(project)

      Given("project does not have Graph Services hook in GitLab")
      `GET <gitlabApi>/projects/:id/hooks returning OK with no hooks`(projectId)

      Given("project creation in GitLab returning CREATED")
      `POST <gitlabApi>/projects/:id/hooks returning CREATED`(projectId)

      Given("some Commit exists for the project in GitLab")
      givenAccessTokenPresentFor(project)
      val commitId  = commitIds.generateOne
      val committer = personEntities.generateOne
      `GET <gitlabApi>/projects/:id/repository/commits returning OK with a commit`(projectId, commitId)
      `GET <gitlabApi>/projects/:id/repository/commits/:sha returning OK with some event`(projectId, commitId)

      // making the triples generation be happy and not throwing exceptions to the logs
      `GET <gitlabApi>/projects/:path returning OK with`(project)
      `GET <triples-generator>/projects/:id/commits/:id returning OK with some triples`(project, commitId, committer)
      `GET <gitlabApi>/projects/:path/members returning OK with the list of members`(project)

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
