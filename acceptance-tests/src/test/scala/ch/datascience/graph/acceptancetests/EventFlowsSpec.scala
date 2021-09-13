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
import ch.datascience.graph.acceptancetests.db.EventLog
import ch.datascience.graph.acceptancetests.flows.AccessTokenPresence.givenAccessTokenPresentFor
import ch.datascience.graph.acceptancetests.flows.RdfStoreProvisioning.`wait for events to be processed`
import ch.datascience.graph.acceptancetests.stubs.GitLab._
import ch.datascience.graph.acceptancetests.stubs.RemoteTriplesGenerator._
import ch.datascience.graph.acceptancetests.tooling._
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.http.client.AccessToken
import ch.datascience.webhookservice.model.HookToken
import org.http4s.Status._
import org.scalatest.GivenWhenThen
import org.scalatest.concurrent.Eventually
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should
import org.scalatest.time.{Millis, Minutes, Span}

class EventFlowsSpec
    extends AnyFeatureSpec
    with ModelImplicits
    with GivenWhenThen
    with GraphServices
    with Eventually
    with should.Matchers {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(Span(2, Minutes)),
    interval = scaled(Span(500, Millis))
  )

  Feature("Push events from GitLab should be translated into triples in the RDF Store") {

    Scenario("Valid events get through to the RDF store") {

      implicit val accessToken: AccessToken = accessTokens.generateOne
      val project  = dataProjects(projectEntities(visibilityPublic)).generateOne
      val commitId = commitIds.generateOne

      Given("commit with the commit id matching Push Event's 'after' exists on the project in GitLab")
      `GET <gitlabApi>/projects/:id/repository/commits/:sha returning OK with some event`(project.id, commitId)
      `GET <gitlabApi>/projects/:id/repository/commits per page returning OK with a commit`(project.id, commitId)

      And("RDF triples are generated by the Remote Triples Generator")
      `GET <triples-generator>/projects/:id/commits/:id returning OK with some triples`(project, commitId)

      And("project exists in GitLab")
      `GET <gitlabApi>/projects/:path AND :id returning OK with`(project)

      And("access token is present")
      givenAccessTokenPresentFor(project)

      When("a Push Event arrives")
      webhookServiceClient
        .POST("webhooks/events", HookToken(project.id), data.GitLab.pushEvent(project, commitId))
        .status shouldBe Accepted

      And("relevant commit events are processed")
      `wait for events to be processed`(project.id)

      Then(s"all the events should get the $TriplesStore status in the Event Log")
      EventLog.findEvents(project.id).map(_._2).toSet shouldBe Set(TriplesStore)
    }

    Scenario("A non recoverable generation error arises and the events are reported as failed") {

      implicit val accessToken: AccessToken = accessTokens.generateOne
      val project   = dataProjects(projectEntities(visibilityPublic)).generateOne
      val projectId = project.id
      val commitId  = commitIds.generateOne

      Given("commit with the commit id matching Push Event's 'after' exists on the project in GitLab")
      `GET <gitlabApi>/projects/:id/repository/commits/:sha returning OK with some event`(projectId, commitId)
      `GET <gitlabApi>/projects/:id/repository/commits per page returning OK with a commit`(projectId, commitId)

      And("the Remote Triples Generator return a non recoverable status")
      `GET <triples-generator>/projects/:id/commits/:id fails non recoverably`(project, commitId)

      And("project exists in GitLab")
      `GET <gitlabApi>/projects/:path AND :id returning OK with`(project)

      And("access token is present")
      givenAccessTokenPresentFor(project)

      When("a Push Event arrives")
      webhookServiceClient
        .POST("webhooks/events", HookToken(projectId), data.GitLab.pushEvent(project, commitId))
        .status shouldBe Accepted

      And("relevant commit events are processed")
      `wait for events to be processed`(project.id)

      And(s"all the events should get the $GenerationNonRecoverableFailure status in the Event Log")
      EventLog.findEvents(projectId).map(_._2).toSet shouldBe Set(GenerationNonRecoverableFailure)
    }

    Scenario(
      "A recoverable error arises and the events are reported as a recoverable failure"
    ) {

      implicit val accessToken: AccessToken = accessTokens.generateOne
      val project  = dataProjects(projectEntities(visibilityPublic)).generateOne
      val commitId = commitIds.generateOne

      Given("commit with the commit id matching Push Event's 'after' exists on the project in GitLab")
      `GET <gitlabApi>/projects/:id/repository/commits/:sha returning OK with some event`(project.id, commitId)
      `GET <gitlabApi>/projects/:id/repository/commits per page returning OK with a commit`(project.id, commitId)

      And("The remote triples generator fails recoverably")
      `GET <triples-generator>/projects/:id/commits/:id fails recoverably`(project, commitId)

      And("project exists in GitLab")
      `GET <gitlabApi>/projects/:path AND :id returning OK with`(project)

      And("access token is present")
      givenAccessTokenPresentFor(project)

      When("a Push Event arrives")
      webhookServiceClient
        .POST("webhooks/events", HookToken(project.id), data.GitLab.pushEvent(project, commitId))
        .status shouldBe Accepted

      Then(s"all the events should get the $GenerationRecoverableFailure status in the Event Log")
      eventually {
        EventLog.findEvents(project.id).map(_._2).toSet shouldBe Set(GenerationRecoverableFailure)
      }
    }

    Scenario("A non recoverable transformation error arises and the events are reported as a non recoverable failure") {

      implicit val accessToken: AccessToken = accessTokens.generateOne
      val project  = dataProjects(projectEntities(visibilityPublic)).generateOne
      val commitId = commitIds.generateOne

      Given("commit with the commit id matching Push Event's 'after' exists on the project in GitLab")
      `GET <gitlabApi>/projects/:id/repository/commits/:sha returning OK with some event`(project.id, commitId)
      `GET <gitlabApi>/projects/:id/repository/commits per page returning OK with a commit`(project.id, commitId)

      And("RDF triples are generated by the Remote Triples Generator")
      `GET <triples-generator>/projects/:id/commits/:id returning OK with some triples`(project, commitId)

      And("the first time the project is found")
      `GET <gitlabApi>/projects/:path AND :id returning OK with`(project)

      And("access token is present")
      givenAccessTokenPresentFor(project)

      And("the transformation failed non recoverably")
      `GET <gitlabApi>/projects/:path returning BadRequest`(project)

      When("a Push Event arrives")
      webhookServiceClient
        .POST("webhooks/events", HookToken(project.id), data.GitLab.pushEvent(project, commitId))
        .status shouldBe Accepted

      And("relevant commit events are processed")
      `wait for events to be processed`(project.id)

      Then(s"all the events should get the $TransformationNonRecoverableFailure status in the Event Log")
      EventLog.findEvents(project.id).map(_._2).toSet shouldBe Set(TransformationNonRecoverableFailure)
    }
  }
}
