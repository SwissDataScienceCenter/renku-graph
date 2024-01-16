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

package io.renku.graph.acceptancetests.knowledgegraph

import cats.syntax.all._
import io.circe.Json
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.data._
import io.renku.graph.acceptancetests.flows.TSProvisioning
import io.renku.graph.acceptancetests.tooling.{AcceptanceSpec, ApplicationServices}
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.projects.{Role, Visibility}
import io.renku.graph.model.testentities._
import io.renku.http.RenkuEntityCodec
import io.renku.http.rest.Links
import io.renku.http.server.EndpointTester.JsonOps
import org.http4s.Status._
import org.scalatest.EitherValues

class ProjectsResourcesSpec
    extends AcceptanceSpec
    with ApplicationServices
    with TSProvisioning
    with DatasetsApiEncoders
    with RenkuEntityCodec
    with EitherValues {

  private val user        = authUsers.generateOne
  private val accessToken = user.accessToken

  private val parentProject -> project = {
    val creatorGitLabId = personGitLabIds.generateOne
    val creatorEmail    = personEmails.generateOne
    val creator         = cliShapedPersons.generateOne.copy(maybeEmail = creatorEmail.some)

    val (parent, child) = renkuProjectEntities(visibilityPublic, creatorGen = cliShapedPersons)
      .withDatasets(datasetEntities(provenanceInternal(cliShapedPersons)))
      .generateOne
      .forkOnce(creatorsGen = cliShapedPersons)
      .bimap(removeMembers(), removeMembers())

    val parentDataProject = dataProjects(parent)
      .map(replaceCreatorFrom(creator, creatorGitLabId))
      .map(addMemberFrom(creator, creatorGitLabId, Role.Owner) >>> addMemberWithId(user.id, Role.Maintainer))
      .generateOne

    val childDataProject =
      dataProjects(child.copy(visibility = Visibility.Private, parent = parentDataProject.entitiesProject))
        .map(addMemberWithId(user.id, Role.Owner))
        .generateOne

    parentDataProject -> childDataProject
  }

  Feature("GET knowledge-graph/projects/<namespace>/<name> to find project's details") {

    Scenario("As a user I would like to find project's details by calling a REST endpoint") {
      Given("the user is authenticated")
      gitLabStub.addAuthenticated(user)

      And("there are some data in the Triples Store")
      val parentCommitId = commitIds.generateOne
      mockCommitDataOnTripleGenerator(parentProject, toPayloadJsonLD(parentProject), parentCommitId)
      gitLabStub.setupProject(parentProject, parentCommitId)
      `data in the Triples Store`(parentProject, parentCommitId, accessToken)
      waitForAllEventsInFinalState(parentProject.id)

      val commitId    = commitIds.generateOne
      val jsonPayload = toPayloadJsonLD(project.entitiesProject)
      mockCommitDataOnTripleGenerator(project, jsonPayload, commitId)
      gitLabStub.setupProject(project, commitId)
      `data in the Triples Store`(project, commitId, accessToken)
      waitForAllEventsInFinalState(project.id)

      When("the user fetches project's details with GET knowledge-graph/projects/<namespace>/<name>")
      val projectDetailsResponse = knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.slug}", accessToken)

      Then("he should get OK response with project's details")
      projectDetailsResponse.status shouldBe Ok
      val projectDetails = projectDetailsResponse.jsonBody
      projectDetails shouldBe fullJson(project)(gitLabUrl)

      When("user then fetches project's datasets using the link from the response")
      val datasetsLink = projectDetails._links
        .fold(throw _, identity)
        .get(Links.Rel("datasets"))
        .getOrElse(fail("No link with rel 'datasets'"))

      val datasetsResponseStatus -> datasetsResponseBody = restClient
        .GET(datasetsLink.href.toString, accessToken)
        .flatMap(response => response.as[Json].map(json => response.status -> json))
        .unsafeRunSync()

      Then("he should get OK response with the projects datasets")
      datasetsResponseStatus shouldBe Ok
      val foundDatasets = datasetsResponseBody.as[List[Json]].value
      foundDatasets should contain theSameElementsAs project.entitiesProject.datasets.map(briefJson(_, project.slug))

      When("there's an authenticated user who is not project member")
      val nonMemberUser = authUsers.generateOne
      gitLabStub.addAuthenticated(nonMemberUser)

      And("he fetches project's details")
      val projectDetailsResponseForNonMember =
        knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.slug}", nonMemberUser.accessToken)

      Then("he should get NOT_FOUND response")
      projectDetailsResponseForNonMember.status shouldBe NotFound

      When("the user calls the Delete Project API")
      knowledgeGraphClient.DELETE(s"knowledge-graph/projects/${project.slug}", accessToken).status shouldBe Accepted

      Then("the project should be deleted")
      eventually {
        knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.slug}", accessToken).status shouldBe NotFound
      }
    }
  }
}
