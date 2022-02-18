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

package io.renku.graph.acceptancetests.knowledgegraph

import io.circe.literal._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.flows.RdfStoreProvisioning
import io.renku.graph.acceptancetests.tooling.GraphServices
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.testentities._
import io.renku.graph.model.testentities.RenkuProject._
import io.renku.jsonld.syntax._
import io.renku.graph.acceptancetests.data.{Project, _}
import io.renku.http.server.EndpointTester.{JsonOps, jsonEntityDecoder}
import io.renku.http.client.AccessToken
import org.http4s.Status._
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import eu.timepit.refined.auto._
import io.circe.Json
import io.renku.http.rest.Links
import io.renku.webhookservice.model
import java.lang.Thread.sleep
import scala.concurrent.duration._
import io.renku.graph.acceptancetests.db.EventLog
import io.renku.graph.model.events

class ProjectReProvisioningSpec
    extends AnyFeatureSpec
    with GivenWhenThen
    with GraphServices
    with RdfStoreProvisioning
    with DatasetsResources {

  private val user = authUsers.generateOne
  private implicit val accessToken: AccessToken = user.accessToken

  Feature("Project re-provisioning") {

    Scenario("A change in the commit history should trigger a re-provisioning") {

      val project = dataProjects(renkuProjectEntities(visibilityPublic)).generateOne
      val commits = commitIds.generateNonEmptyList(minElements = 3)

      Given("there is data in the triple store")

      `GET <gitlabApi>/user returning OK`(user)

      mockDataOnGitLabAPIs(project, project.entitiesProject.asJsonLD, commits)
      `data in the RDF store`(project, commits.last)

      eventually {
        EventLog.findEvents(project.id, events.EventStatus.TriplesStore).toSet shouldBe commits.toList.toSet
      }

      AssertProjectDataIsCorrect(project, project.entitiesProject)

      When("the commit history changes")


      val newCommits  = commitIds.generateNonEmptyList(minElements = 3)
      val newEntities = generateNewActivitiesAndDataset(project.entitiesProject)

      mockDataOnGitLabAPIs(project, newEntities.asJsonLD, newCommits)

      commits.toList.foreach { commitId =>
        `GET <gitlabApi>/projects/:id/repository/commits/:sha returning NOT_FOUND`(project, commitId)
      }

      webhookServiceClient
        .POST("webhooks/events", model.HookToken(project.id), GitLab.pushEvent(project, newCommits.last))
        .status shouldBe Accepted

      sleep((3 second).toMillis)

      `wait for events to be processed`(project.id)

      eventually {
        EventLog.findEvents(project.id, events.EventStatus.TriplesStore).toSet shouldBe newCommits.toList.toSet
      }

      Then("the project should show the new data")

      AssertProjectDataIsCorrect(project, newEntities)
    }

    Scenario("Removing a project from GitLab should remove it from the knowledge-graph") {

      val project = dataProjects(renkuProjectEntities(visibilityPublic)).generateOne
      val commits = commitIds.generateNonEmptyList(minElements = 3)

      Given("There is data in the triple store")

      `GET <gitlabApi>/user returning OK`(user)

      mockDataOnGitLabAPIs(project, project.entitiesProject.asJsonLD, commits)
      `data in the RDF store`(project, commits)

      AssertProjectDataIsCorrect(project, project.entitiesProject)

      When("the project is removed from GitLab")

      commits.toList.foreach { commitId =>
        `GET <gitlabApi>/projects/:id/repository/commits/:sha returning NOT_FOUND`(project, commitId)
      }

      `GET <gitlabApi>/projects/:path AND :id returning NOT_FOUND`(project)

      `GET <gitlabApi>/projects/:id/repository/commits per page returning NOT_FOUND`(project.id)

      `GET <gitlabApi>/projects/:id/events?action=pushed&page=1 returning NOT_FOUND`(
        project
      )

      And("the global commit sync is triggered")
      EventLog.removeGlobalCommitSyncRow(project.id)

      `wait for events to be processed`(project.id)

      Then("the project and its datasets should be removed from the knowledge-graph")

      val projectDetailsResponse = knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.path}", accessToken)

      projectDetailsResponse.status shouldBe NotFound

      project.entitiesProject.datasets.foreach { dataset =>
        val datasetsResponse =
          knowledgeGraphClient.GET(s"knowledge-graph/datasets/${dataset.identification.identifier}", accessToken)

        datasetsResponse shouldBe NotFound
      }

    }
  }

  private def AssertProjectDataIsCorrect(project: Project, projectEntities: RenkuProject)(implicit
      accessToken:                                AccessToken
  ) = {

    val projectDetailsResponse = knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.path}", accessToken)

    projectDetailsResponse.status shouldBe Ok
    val projectDetails = projectDetailsResponse.jsonBody
    projectDetails shouldBe fullJson(project)

    val datasetsLink = projectDetails._links.fold(throw _, identity).get(Links.Rel("datasets")) getOrElse fail(
      "No link with rel 'datasets'"
    )
    val datasetsResponse = restClient
      .GET(datasetsLink.href.toString, accessToken)
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
