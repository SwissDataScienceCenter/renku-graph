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

import cats.syntax.all._
import io.circe.literal._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.flows.TSProvisioning
import io.renku.graph.acceptancetests.tooling.GraphServices
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.testentities.RenkuProject._
import io.renku.graph.model.testentities._
import io.renku.graph.acceptancetests.data._
import io.renku.http.server.EndpointTester.{JsonOps, jsonEntityDecoder}
import io.renku.http.client.AccessToken
import io.renku.jsonld.syntax._
import org.http4s.Status._
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import eu.timepit.refined.auto._
import io.circe.Json
import io.renku.http.rest.Links

class ProjectsResourcesSpec
    extends AnyFeatureSpec
    with GivenWhenThen
    with GraphServices
    with TSProvisioning
    with DatasetsResources {

  private val user = authUsers.generateOne
  private implicit val accessToken: AccessToken = user.accessToken

  private val (parentProject, project) = {
    val creator = personEntities(withGitLabId, withEmail).generateOne
    val (parent, child) = renkuProjectEntities(visibilityPublic)
      .withDatasets(datasetEntities(provenanceInternal))
      .generateOne
      .copy(maybeCreator = creator.some, members = personEntities(withGitLabId).generateFixedSizeSet() + creator)
      .forkOnce()

    (dataProjects(parent).generateOne,
     dataProjects(
       child.copy(visibility = Visibility.Private,
                  members = child.members + personEntities.generateOne.copy(maybeGitLabId = user.id.some)
       )
     ).generateOne
    )
  }

  Feature("GET knowledge-graph/projects/<namespace>/<name> to find project's details") {

    Scenario("As a user I would like to find project's details by calling a REST endpoint") {

      Given("the user is authenticated")
      `GET <gitlabApi>/user returning OK`(user)

      And("there are some data in the Triples Store")
      val parentCommitId = commitIds.generateOne
      mockDataOnGitLabAPIs(parentProject, parentProject.entitiesProject.asJsonLD, parentCommitId)
      `data in the Triples Store`(parentProject, parentCommitId)

      val commitId = commitIds.generateOne
      mockDataOnGitLabAPIs(project, project.entitiesProject.asJsonLD, commitId)
      `data in the Triples Store`(project, commitId)

      When("the user fetches project's details with GET knowledge-graph/projects/<namespace>/<name>")
      val projectDetailsResponse = knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.path}", accessToken)

      Then("he should get OK response with project's details")
      projectDetailsResponse.status shouldBe Ok
      val projectDetails = projectDetailsResponse.jsonBody
      projectDetails shouldBe fullJson(project)

      When("user then fetches project's datasets using the link from the response")
      val datasetsLink = projectDetails._links.fold(throw _, identity).get(Links.Rel("datasets")) getOrElse fail(
        "No link with rel 'datasets'"
      )
      val datasetsResponse = restClient
        .GET(datasetsLink.href.toString, accessToken)
        .flatMap(response => response.as[Json].map(json => response.status -> json))
        .unsafeRunSync()

      Then("he should get OK response with the projects datasets")
      datasetsResponse._1 shouldBe Ok
      val Right(foundDatasets) = datasetsResponse._2.as[List[Json]]
      foundDatasets should contain theSameElementsAs project.entitiesProject.datasets.map(briefJson(_, project.path))

      When("there's an authenticated user who is not project member")
      val nonMemberAccessToken = accessTokens.generateOne
      `GET <gitlabApi>/user returning OK`()(nonMemberAccessToken)

      And("he fetches project's details")
      val projectDetailsResponseForNonMember =
        knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.path}", nonMemberAccessToken)

      Then("he should get NOT_FOUND response")
      projectDetailsResponseForNonMember.status shouldBe NotFound
    }
  }

}
