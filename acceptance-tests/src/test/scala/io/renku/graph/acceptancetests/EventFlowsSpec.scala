/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

import data._
import db.EventLog
import flows.TSProvisioning
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.testentities.cliShapedPersons
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.webhookservice.model.HookToken
import org.http4s.Status._
import tooling._

class EventFlowsSpec extends AcceptanceSpec with ApplicationServices with TSProvisioning {

  Feature("Push events from GitLab should be translated into triples in the Triples Store") {

    Scenario("Valid events get through to the Triples Store") {

      val user = authUsers.generateOne
      val project = dataProjects(
        renkuProjectEntities(visibilityPublic, creatorGen = cliShapedPersons).modify(removeMembers())
      ).map(addMemberWithId(user.id)).generateOne
      val commitId = commitIds.generateOne

      Given("commit with the commit id matching Push Event's 'after' exists on the project in GitLab")
      gitLabStub.addAuthenticated(user)
      gitLabStub.setupProject(project, commitId)

      And("triples are generated by the Remote Triples Generator")
      `GET <triples-generator>/projects/:id/commits/:id returning OK with some triples`(project, commitId)

      And("access token is present")
      givenAccessTokenPresentFor(project, user.accessToken)

      When("a Push Event arrives")
      webhookServiceClient
        .POST("webhooks/events", HookToken(project.id), data.GitLab.pushEvent(project, commitId))
        .status shouldBe Accepted

      And("commit events are processed")
      `wait for events to be processed`(project.id, user.accessToken, 5)

      Then(s"all the events should get the $TriplesStore status in the Event Log")
      EventLog.findEvents(project.id).map(_._2).toSet shouldBe Set(TriplesStore)
    }

    Scenario("A non recoverable generation error arises and the events are reported as failed") {

      val user = authUsers.generateOne
      val project = dataProjects(
        renkuProjectEntities(visibilityPublic, creatorGen = cliShapedPersons).modify(removeMembers())
      ).map(addMemberWithId(user.id)).generateOne
      val commitId = commitIds.generateOne

      Given("commit with the commit id matching Push Event's 'after' exists on the project in GitLab")
      gitLabStub.addAuthenticated(user)
      gitLabStub.setupProject(project, commitId)

      And("the Remote Triples Generator return a non recoverable status")
      `GET <triples-generator>/projects/:id/commits/:id fails non recoverably`(project, commitId)

      And("access token is present")
      givenAccessTokenPresentFor(project, user.accessToken)

      When("a Push Event arrives")
      webhookServiceClient
        .POST("webhooks/events", HookToken(project.id), data.GitLab.pushEvent(project, commitId))
        .status shouldBe Accepted

      And("relevant commit events are processed")
      `wait for events to be processed`(project.id, user.accessToken, 5)

      And(s"all the events should get the $GenerationNonRecoverableFailure status in the Event Log")
      EventLog.findEvents(project.id).map(_._2).toSet shouldBe Set(GenerationNonRecoverableFailure)
    }

    Scenario("A recoverable error arises and the events are reported as a recoverable failure") {

      val user = authUsers.generateOne
      val project = dataProjects(
        renkuProjectEntities(visibilityPublic, creatorGen = cliShapedPersons).modify(removeMembers())
      ).map(addMemberWithId(user.id)).generateOne
      val commitId = commitIds.generateOne

      Given("commit with the commit id matching Push Event's 'after' exists on the project in GitLab")
      gitLabStub.addAuthenticated(user)
      gitLabStub.setupProject(project, commitId)

      And("The remote triples generator fails recoverably")
      `GET <triples-generator>/projects/:id/commits/:id fails recoverably`(project, commitId)

      And("access token is present")
      givenAccessTokenPresentFor(project, user.accessToken)

      When("a Push Event arrives")
      webhookServiceClient
        .POST("webhooks/events", HookToken(project.id), data.GitLab.pushEvent(project, commitId))
        .status shouldBe Accepted

      Then(s"all the events should get the $GenerationRecoverableFailure status in the Event Log")
      eventually {
        EventLog.findEvents(project.id).map(_._2).toSet shouldBe Set(GenerationRecoverableFailure)
      }

      // Removing the error so the logs are not polluted with unauthorized exceptions
      `GET <triples-generator>/projects/:id/commits/:id returning OK with some triples`(project, commitId)
    }

    Scenario("A non recoverable transformation error arises and the events are reported as a non recoverable failure") {

      val user = authUsers.generateOne
      val project = dataProjects(
        renkuProjectEntities(visibilityPublic, creatorGen = cliShapedPersons).modify(removeMembers())
      ).map(addMemberWithId(user.id)).generateOne
      val commitId = commitIds.generateOne

      Given("commit with the commit id matching Push Event's 'after' exists on the project in GitLab")
      gitLabStub.addAuthenticated(user)
      gitLabStub.setupProject(project, commitId)

      And("access token is present")
      givenAccessTokenPresentFor(project, user.accessToken)

      And("triples generation returns malformed payload")
      `GET <triples-generator>/projects/:id/commits/:id returning OK with broken payload`(project, commitId)

      When("a Push Event arrives")
      webhookServiceClient
        .POST("webhooks/events", HookToken(project.id), data.GitLab.pushEvent(project, commitId))
        .status shouldBe Accepted

      And("relevant commit events are processed")
      `wait for events to be processed`(project.id, user.accessToken, 5)

      Then(s"all the events should get the $TransformationNonRecoverableFailure status in the Event Log")
      EventLog.findEvents(project.id).map(_._2).toSet shouldBe Set(TransformationNonRecoverableFailure)
    }
  }
}
