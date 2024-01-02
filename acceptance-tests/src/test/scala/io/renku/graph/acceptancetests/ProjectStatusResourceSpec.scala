/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.circe.Json
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.data.Project.Statistics.CommitsCount
import io.renku.graph.acceptancetests.data._
import io.renku.graph.acceptancetests.flows.{AccessTokenPresence, TSProvisioning}
import io.renku.graph.acceptancetests.testing.AcceptanceTestPatience
import io.renku.graph.acceptancetests.tooling.{AcceptanceSpec, ApplicationServices, ModelImplicits}
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.events.EventStatusProgress
import io.renku.graph.model.projects.Role
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import org.http4s.Status._
import org.scalatest.concurrent.Eventually

class ProjectStatusResourceSpec
    extends AcceptanceSpec
    with ModelImplicits
    with ApplicationServices
    with TSProvisioning
    with AccessTokenPresence
    with Eventually
    with AcceptanceTestPatience {

  private val numberOfEvents: Int Refined Positive = 5

  Feature("Project Status API for a given project") {

    val memberUser = authUsers.generateOne
    val project =
      dataProjects(renkuProjectEntities(visibilityPublic, creatorGen = cliShapedPersons).modify(removeMembers()),
                   CommitsCount(numberOfEvents.value)
      ).map(addMemberWithId(memberUser.id, Role.Owner)).generateOne

    Scenario("Call by a user who is not a member of the project") {

      Given("there's no webhook for a given project in GitLab")
      gitLabStub.addProject(project)

      And("project hasn't been activated (no trace of it in the EL)")

      And("the user is authenticated")
      val someAuthUser = authUsers.generateOne
      gitLabStub.addAuthenticated(someAuthUser)

      When("the user who is not a member of the project is calling the Status API")
      Then("the Status API should return NOT_FOUND")
      webhookServiceClient
        .`GET projects/:id/events/status`(project.id, someAuthUser.accessToken)
        .status shouldBe NotFound

      When("a member of the project activates it")
      gitLabStub.addAuthenticated(memberUser)
      val commitId = commitIds.generateOne
      gitLabStub.replaceCommits(project.id, commitId)
      mockCommitDataOnTripleGenerator(project, toPayloadJsonLD(project), commitId)

      webhookServiceClient.`POST projects/:id/webhooks`(project.id, memberUser.accessToken).status shouldBe Created

      Then("the non-member user should get OK with 'activated' = true")
      eventually {
        val response = webhookServiceClient.`GET projects/:id/events/status`(project.id, someAuthUser.accessToken)
        response.status                                                    shouldBe Ok
        response.jsonBody.hcursor.downField("activated").as[Boolean].value shouldBe true
      }
    }

    Scenario("Call by a user who is a member of the project") {

      When("there's no webhook for a given project in GitLab")
      gitLabStub.addProject(project)

      Given("the member is authenticated")
      gitLabStub.addAuthenticated(memberUser)

      Then("the member of the project calling the Status API should get OK with 'activated' = false")
      val response = webhookServiceClient.`GET projects/:id/events/status`(project.id, memberUser.accessToken)
      response.status                                                    shouldBe Ok
      response.jsonBody.hcursor.downField("activated").as[Boolean].value shouldBe false

      When("there's a webhook created for the project")
      And("there are events under processing")
      val allCommitIds = commitIds.generateNonEmptyList(min = numberOfEvents, max = numberOfEvents)
      gitLabStub.replaceCommits(project.id, allCommitIds.toList: _*)
      mockCommitDataOnTripleGenerator(project, toPayloadJsonLD(project), allCommitIds)

      webhookServiceClient.`POST projects/:id/webhooks`(project.id, memberUser.accessToken).status shouldBe Created

      `data in the Triples Store`(project, allCommitIds, memberUser.accessToken)

      Then("the Status API should return OK with some status info")
      eventually {
        val response = webhookServiceClient.`GET projects/:id/events/status`(project.id, memberUser.accessToken)

        response.status shouldBe Ok

        val responseJson = response.jsonBody.hcursor
        responseJson.downField("activated").as[Boolean].value shouldBe true

        val progressObjCursor = responseJson.downField("progress").as[Json].fold(throw _, identity).hcursor
        progressObjCursor.downField("done").as[Int].value         shouldBe EventStatusProgress.Stage.Final.value
        progressObjCursor.downField("total").as[Int].value        shouldBe EventStatusProgress.Stage.Final.value
        progressObjCursor.downField("percentage").as[Float].value shouldBe 100f

        val detailsObjCursor = responseJson.downField("details").as[Json].fold(throw _, identity).hcursor
        detailsObjCursor.downField("status").as[String].value  shouldBe "success"
        detailsObjCursor.downField("message").as[String].value shouldBe "triples store"
      }
    }
  }
}
