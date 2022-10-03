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

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Json
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.data.{Project, _}
import io.renku.graph.acceptancetests.db.EventLog
import io.renku.graph.acceptancetests.flows.TSProvisioning
import io.renku.graph.acceptancetests.knowledgegraph.{DatasetsApiEncoders, fullJson}
import io.renku.graph.acceptancetests.tooling.{AcceptanceSpec, ApplicationServices}
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.{GraphClass, events}
import io.renku.graph.model.testentities.RenkuProject._
import io.renku.graph.model.testentities._
import io.renku.http.client.AccessToken
import io.renku.http.rest.Links
import io.renku.http.server.EndpointTester.{JsonOps, jsonEntityDecoder}
import io.renku.jsonld.syntax._
import io.renku.webhookservice.model
import org.http4s.Status._

import java.lang.Thread.sleep
import scala.concurrent.duration._

class CommitHistoryChangesSpec
    extends AcceptanceSpec
    with ApplicationServices
    with TSProvisioning
    with DatasetsApiEncoders {

  private implicit val graph: GraphClass = GraphClass.Default
  private val user = authUsers.generateOne

  Feature("Changes in the commit history to trigger re-provisioning") {

    Scenario("A change in the commit history should trigger the re-provisioning process") {

      val project = dataProjects(
        renkuProjectEntities(visibilityPublic).withDatasets(datasetEntities(provenanceInternal))
      ).generateOne
      val commits = commitIds.generateNonEmptyList(minElements = 3)

      Given("there is data in the TS")

      gitLabStub.addAuthenticated(user)
      gitLabStub.setupProject(project, commits.toList: _*)
      mockCommitDataOnTripleGenerator(project, project.entitiesProject.asJsonLD, commits)

      `data in the Triples Store`(project, commits, user.accessToken)

      eventually {
        EventLog.findEvents(project.id, events.EventStatus.TriplesStore).toSet shouldBe commits.toList.toSet
      }

      assertProjectDataIsCorrect(project, project.entitiesProject, user.accessToken)

      When("the commit history changes")

      val newCommits  = commitIds.generateNonEmptyList(minElements = 3)
      val newEntities = generateNewActivitiesAndDataset(project.entitiesProject)

      gitLabStub.replaceCommits(project.id, newCommits.toList: _*)
      mockCommitDataOnTripleGenerator(project, newEntities.asJsonLD, newCommits)

      webhookServiceClient
        .POST("webhooks/events", model.HookToken(project.id), GitLab.pushEvent(project, newCommits.last))
        .status shouldBe Accepted

      sleep((10 seconds).toMillis)

      `wait for events to be processed`(project.id)

      eventually {
        EventLog.findEvents(project.id, events.EventStatus.TriplesStore).toSet shouldBe newCommits.toList.toSet
      }

      Then("the project should contain the new data")
      assertProjectDataIsCorrect(project, newEntities, user.accessToken)
    }

    Scenario("Removing a project from GitLab should remove it from the knowledge-graph") {

      val project = dataProjects(renkuProjectEntities(visibilityPublic)).generateOne
      val commits = commitIds.generateNonEmptyList(minElements = 3)

      Given("There is data in the triple store")

      gitLabStub.addAuthenticated(user)
      gitLabStub.setupProject(project, commits.toList: _*)

      mockCommitDataOnTripleGenerator(project, project.entitiesProject.asJsonLD, commits)
      `data in the Triples Store`(project, commits, user.accessToken)

      assertProjectDataIsCorrect(project, project.entitiesProject, user.accessToken)

      When("the project is removed from GitLab")
      gitLabStub.removeProject(project.id)

      And("the global commit sync is triggered")
      EventLog.removeGlobalCommitSyncRow(project.id)

      sleep((1 second).toMillis)

      `wait for events to be processed`(project.id)

      Then("the project and its datasets should be removed from the knowledge-graph")

      knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.path}", user.accessToken).status shouldBe NotFound

      project.entitiesProject.datasets.foreach { dataset =>
        knowledgeGraphClient
          .GET(s"knowledge-graph/datasets/${dataset.identification.identifier}", user.accessToken)
          .status shouldBe NotFound
      }
    }
  }

  private def assertProjectDataIsCorrect(project: Project, projectEntities: RenkuProject, accessToken: AccessToken) = {

    val projectDetailsResponse = knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.path}", accessToken)

    projectDetailsResponse.status shouldBe Ok
    val projectDetails = projectDetailsResponse.jsonBody
    projectDetails shouldBe fullJson(project)

    val datasetsLink = projectDetails._links
      .fold(throw _, identity)
      .get(Links.Rel("datasets"))
      .getOrElse(fail("No link with rel 'datasets'"))

    val datasetsResponse = restClient
      .GET(datasetsLink.href.show, accessToken)
      .flatMap(response => response.as[Json].map(json => response.status -> json))
      .unsafeRunSync()

    datasetsResponse._1 shouldBe Ok
    val Right(foundDatasets) = datasetsResponse._2.as[List[Json]]
    foundDatasets should contain theSameElementsAs projectEntities.datasets.map(briefJson(_, project.path))
  }

  private def generateNewActivitiesAndDataset(projectEntities: RenkuProject) =
    renkuProjectEntities(visibilityPublic).generateOne.copy(
      version = projectEntities.version,
      path = projectEntities.path,
      name = projectEntities.name,
      maybeDescription = projectEntities.maybeDescription,
      agent = projectEntities.agent,
      dateCreated = projectEntities.dateCreated,
      maybeCreator = projectEntities.maybeCreator,
      keywords = projectEntities.keywords,
      members = projectEntities.members
    )
}
