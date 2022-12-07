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

import io.circe.syntax.EncoderOps
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.data.Project.Statistics.CommitsCount
import io.renku.graph.acceptancetests.data.dataProjects
import io.renku.graph.acceptancetests.flows.AccessTokenPresence
import io.renku.graph.acceptancetests.tooling.{AcceptanceSpec, ApplicationServices, ModelImplicits}
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.http.client.AccessToken
import org.http4s.Status._

class WebhookCreationSpec extends AcceptanceSpec with ModelImplicits with ApplicationServices with AccessTokenPresence {

  Feature("A Graph Services hook can be created for a project") {

    Scenario("Graph Services hook is present on the project in GitLab") {

      val project = dataProjects(renkuProjectEntities(visibilityPublic), CommitsCount.zero).generateOne

      val user = authUsers.generateOne
      Given("api user is authenticated")
      gitLabStub.addAuthenticated(user)

      Given("project is present in GitLab with has Graph Services hook")
      gitLabStub.setupProject(project)

      When("user does POST webhook-service/projects/:id/webhooks")
      val response = webhookServiceClient.POST(s"projects/${project.id}/webhooks", user.accessToken)

      Then("he should get OK response back")
      response.status shouldBe Ok
    }

    Scenario("No Graph Services webhook on the project in GitLab") {

      val user    = authUsers.generateOne
      val project = dataProjects(renkuProjectEntities(visibilityPublic), CommitsCount.one).generateOne

      Given("api user is authenticated")
      gitLabStub.addAuthenticated(user)

      Given("project is present in GitLab but no Graph Services hook is present")
      gitLabStub.addProject(project)

      Given("some Commit exists for the project in GitLab")
      givenAccessTokenPresentFor(project, user.accessToken)
      val commitId = commitIds.generateOne
      gitLabStub.replaceCommits(project.id, commitId)

      // making the triples generation be happy and not throwing exceptions to the logs
      `GET <triples-generator>/projects/:id/commits/:id returning OK with some triples`(project, commitId)

      When("user does POST webhook-service/projects/:id/webhooks")
      val response = webhookServiceClient.POST(s"projects/${project.id}/webhooks", user.accessToken)

      Then("he should get CREATED response back")
      response.status shouldBe Created

      And("a Project Access Token should created for the Project and added to the token repository")
      tokenRepositoryClient
        .GET(s"projects/${project.id}/tokens")
        .jsonBody shouldBe gitLabStub
        .query(_.projectAccessTokens(project.id).token)
        .unsafeRunSync()
        .asInstanceOf[AccessToken]
        .asJson
    }
  }
}
