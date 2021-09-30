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
import ch.datascience.graph.acceptancetests.data.Project.Statistics.CommitsCount
import ch.datascience.graph.acceptancetests.data.dataProjects
import ch.datascience.graph.acceptancetests.db.EventLog
import ch.datascience.graph.acceptancetests.flows.AccessTokenPresence.givenAccessTokenPresentFor
import ch.datascience.graph.acceptancetests.flows.RdfStoreProvisioning.`wait for events to be processed`
import ch.datascience.graph.acceptancetests.stubs.GitLab._
import ch.datascience.graph.acceptancetests.stubs.RemoteTriplesGenerator._
import ch.datascience.graph.acceptancetests.testing.AcceptanceTestPatience
import ch.datascience.graph.acceptancetests.tooling.{GraphServices, ModelImplicits}
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.events.EventId
import ch.datascience.graph.model.events.EventStatus.TriplesStore
import ch.datascience.graph.model.projects.Id
import ch.datascience.http.client.AccessToken
import ch.datascience.webhookservice.model.HookToken
import io.renku.eventlog.TypeSerializers
import org.http4s.Status._
import org.scalatest.GivenWhenThen
import org.scalatest.concurrent.Eventually
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should
import skunk.Command
import skunk.implicits._

class CommitSyncFlowsSpec
    extends AnyFeatureSpec
    with ModelImplicits
    with GivenWhenThen
    with GraphServices
    with Eventually
    with AcceptanceTestPatience
    with should.Matchers
    with TypeSerializers {

  Feature("Missed GitLab events should be synchronised") {

    Scenario("There's a commit in GitLab for which there's no event in EL") {

      implicit val accessToken: AccessToken = accessTokens.generateOne
      val project           = dataProjects(projectEntities(visibilityPublic), CommitsCount(2)).generateOne
      val nonMissedCommitId = commitIds.generateOne
      val missedCommitId    = commitIds.generateOne

      Given("commit with the commit id matching Push Event's 'after' exists on the project in GitLab")
      `GET <gitlabApi>/projects/:id/repository/commits/:sha returning OK with some event`(project.id, nonMissedCommitId)
      `GET <gitlabApi>/projects/:id/repository/commits/:sha returning OK with some event`(project.id, missedCommitId)

      And("fetch latest commit endpoint returns the non missed commit")
      `GET <gitlabApi>/projects/:id/repository/commits per page returning OK with a commit`(project.id,
                                                                                            nonMissedCommitId
      )

      And("RDF triples are generated by the Remote Triples Generator for both commits")
      `GET <triples-generator>/projects/:id/commits/:id returning OK with some triples`(project, nonMissedCommitId)
      `GET <triples-generator>/projects/:id/commits/:id returning OK with some triples`(project, missedCommitId)

      And("project exists in GitLab")
      `GET <gitlabApi>/projects/:path AND :id returning OK with`(project)

      And("access token is present")
      givenAccessTokenPresentFor(project)

      When("a Push Event arrives for the non missed event")
      webhookServiceClient
        .POST("webhooks/events", HookToken(project.id), data.GitLab.pushEvent(project, nonMissedCommitId))
        .status shouldBe Accepted

      And("relevant commit events are processed")
      `wait for events to be processed`(project.id)

      Then("the non missed events should be in the RDF Store")
      eventually {
        EventLog.findEvents(project.id) should contain(EventId(nonMissedCommitId.value) -> TriplesStore)
      }

      And("fetch latest commit endpoint returns the missed and the non missed commit")
      `GET <gitlabApi>/projects/:id/repository/commits per page returning OK with a commit`(project.id,
                                                                                            nonMissedCommitId,
                                                                                            missedCommitId
      )

      When("commit synchronisation process kicks-off")
      db.EventLog.execute { session =>
        val query: Command[Id] =
          sql"""
          DELETE FROM subscription_category_sync_time 
          WHERE project_id = $projectIdEncoder AND category_name = 'COMMIT_SYNC'
          """.command
        session.prepare(query).use(_.execute(project.id))
      }

      And("commit events for the missed event are created and processed")
      `wait for events to be processed`(project.id)

      Then("triples for both of the project's commits should be in the RDF Store")
      eventually {
        EventLog.findEvents(project.id) should contain theSameElementsAs List(
          EventId(nonMissedCommitId.value) -> TriplesStore,
          EventId(missedCommitId.value)    -> TriplesStore
        )
      }
    }
  }
}
