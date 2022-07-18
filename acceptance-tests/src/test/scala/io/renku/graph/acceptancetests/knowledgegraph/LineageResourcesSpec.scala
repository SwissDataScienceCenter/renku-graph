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
import io.circe.{ACursor, Json}
import io.renku.generators.CommonGraphGenerators.{accessTokens, authUsers}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.fixed
import io.renku.graph.acceptancetests.data.{TSData, cliVersion, dataProjects}
import io.renku.graph.acceptancetests.flows.TSProvisioning
import io.renku.graph.acceptancetests.tooling.GraphServices
import io.renku.graph.model
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.Schemas.prov
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.testentities.LineageExemplarData.ExemplarData
import io.renku.graph.model.testentities.generators.EntitiesGenerators.{personEntities, renkuProjectEntities, visibilityPublic}
import io.renku.graph.model.testentities.{LineageExemplarData, NodeDef}
import io.renku.http.client.AccessToken
import io.renku.http.client.UrlEncoder.urlEncode
import io.renku.jsonld.syntax._
import org.http4s.Status.{NotFound, Ok}
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec

class LineageResourcesSpec
    extends AnyFeatureSpec
    with GivenWhenThen
    with GraphServices
    with TSProvisioning
    with TSData {

  Feature("GET knowledge-graph/projects/<namespace>/<name>/files/<location>/lineage to find a file's lineage") {
    implicit val accessToken: AccessToken = accessTokens.generateOne

    val (exemplarData, project) = {
      val lineageData = LineageExemplarData(
        renkuProjectEntities(visibilityPublic)
          .map(
            _.copy(
              path = projects.Path("public/lineage-project-for-rest"),
              agent = cliVersion
            )
          )
          .generateOne
      )
      (lineageData, dataProjects(lineageData.project).generateOne)
    }

    /** Expected data structure when looking for the grid_plot file
     *
     * zhbikes folder   clean_data
     *           \      /
     *          run plan 1
     *               \
     *              bikesParquet   plot_data
     *                       \     /
     *                      run plan 2
     *                       /
     *                grid_plot
     */
    Scenario("As a user I would like to find a public project's lineage") {
      Given("some data in the Triples Store")
      val commitId = commitIds.generateOne
      mockDataOnGitLabAPIs(project, exemplarData.project.asJsonLD, commitId)
      `data in the Triples Store`(project, commitId)

      When("user calls the lineage endpoint")
      val response =
        knowledgeGraphClient GET s"knowledge-graph/projects/${project.path}/files/${urlEncode(exemplarData.`grid_plot entity`.location)}/lineage"

      Then("they should get Ok response with project lineage in Json")
      response.status shouldBe Ok
      val lineageJson: ACursor = response.jsonBody.hcursor

      lineageJson.downField("edges").as[List[Json]].map(_.toSet) shouldBe theExpectedEdges(exemplarData)
      lineageJson.downField("nodes").as[List[Json]].map(_.toSet) shouldBe theExpectedNodes(exemplarData)

    }
  }

  Feature("GET knowledge-graph/projects/<namespace>/<name>/files/<location>/lineage to find a file's lineage") {
    val user = authUsers.generateOne
    implicit val accessToken: AccessToken = user.accessToken

    Scenario("As an authenticated user I would like to find lineage of project I am a member of") {
      val accessibleExemplarData = LineageExemplarData(
        renkuProjectEntities(fixed(Visibility.Private)).generateOne.copy(
          path = model.projects.Path("accessible/member-project-for-rest"),
          members = Set(personEntities.generateOne.copy(maybeGitLabId = user.id.some))
        )
      )

      Given("some data in the Triples Store with a project I am a member of")
      val commitId = commitIds.generateOne
      val project  = dataProjects(accessibleExemplarData.project).generateOne
      mockDataOnGitLabAPIs(project, accessibleExemplarData.project.asJsonLD, commitId)
      `data in the Triples Store`(project, commitId)

      And("I am authenticated")
      `GET <gitlabApi>/user returning OK`(user)

      When("user fetches the lineage of the project he is a member of")

      val response =
        knowledgeGraphClient GET (s"knowledge-graph/projects/${project.path}/files/${urlEncode(accessibleExemplarData.`grid_plot entity`.location)}/lineage", user.accessToken)

      Then("he should get OK response with project lineage in Json")
      response.status shouldBe Ok

      val lineageJson: ACursor = response.jsonBody.hcursor

      lineageJson.downField("edges").as[List[Json]].map(_.toSet) shouldBe theExpectedEdges(accessibleExemplarData)
      lineageJson.downField("nodes").as[List[Json]].map(_.toSet) shouldBe theExpectedNodes(accessibleExemplarData)
    }

    Scenario("As an unauthenticated user I should not be able to find a lineage from a private project") {
      val privateExemplarData = LineageExemplarData(
        renkuProjectEntities(fixed(Visibility.Private)).generateOne.copy(
          path = model.projects.Path("private/secret-project-for-rest"),
          members = Set.empty
        )
      )
      val commitId = commitIds.generateOne
      val project  = dataProjects(privateExemplarData.project).generateOne
      mockDataOnGitLabAPIs(project, privateExemplarData.project.asJsonLD, commitId)
      `data in the Triples Store`(project, commitId)

      Given("I am authenticated")
      `GET <gitlabApi>/user returning OK`(user)

      When("user posts a graphql query to fetch lineage of the project he is not a member of")
      val response =
        knowledgeGraphClient.GET(
          s"knowledge-graph/projects/${project.path}/files/${urlEncode(privateExemplarData.`grid_plot entity`.location)}/lineage"
        )

      Then("he should get a NotFound response without lineage")
      response.status                                           shouldBe NotFound
      response.jsonBody.hcursor.downField("message").as[String] shouldBe Right("Resource not found")
    }
  }

  private def theExpectedEdges(exemplarData: ExemplarData): Right[Nothing, Set[Json]] = {
    import exemplarData._
    Right {
      Set(
        json"""{"source": ${`zhbikes folder`.location},     "target": ${`activity3 node`.location}}""",
        json"""{"source": ${`clean_data entity`.location},  "target": ${`activity3 node`.location}}""",
        json"""{"source": ${`activity3 node`.location},     "target": ${`bikesparquet entity`.location}}""",
        json"""{"source": ${`bikesparquet entity`.location},"target": ${`activity4 node`.location}}""",
        json"""{"source": ${`plot_data entity`.location},   "target": ${`activity4 node`.location}}""",
        json"""{"source": ${`activity4 node`.location},     "target": ${`grid_plot entity`.location}}"""
      )
    }
  }

  private def theExpectedNodes(exemplarData: ExemplarData): Right[Nothing, Set[Json]] = {
    import exemplarData._
    Right {
      Set(
        json"""{"id": ${`zhbikes folder`.location},      "location": ${`zhbikes folder`.location},      "label": ${`zhbikes folder`.label},      "type": ${`zhbikes folder`.singleWordType}}""",
        json"""{"id": ${`activity3 node`.location},      "location": ${`activity3 node`.location},     "label": ${`activity3 node`.label},     "type": ${`activity3 node`.singleWordType}}""",
        json"""{"id": ${`clean_data entity`.location},   "location": ${`clean_data entity`.location},   "label": ${`clean_data entity`.label},   "type": ${`clean_data entity`.singleWordType}}""",
        json"""{"id": ${`bikesparquet entity`.location}, "location": ${`bikesparquet entity`.location}, "label": ${`bikesparquet entity`.label}, "type": ${`bikesparquet entity`.singleWordType}}""",
        json"""{"id": ${`plot_data entity`.location},    "location": ${`plot_data entity`.location},    "label": ${`plot_data entity`.label},    "type": ${`plot_data entity`.singleWordType}}""",
        json"""{"id": ${`activity4 node`.location},      "location": ${`activity4 node`.location},     "label": ${`activity4 node`.label},     "type": ${`activity4 node`.singleWordType}}""",
        json"""{"id": ${`grid_plot entity`.location},    "location": ${`grid_plot entity`.location},    "label": ${`grid_plot entity`.label},    "type": ${`grid_plot entity`.singleWordType}}"""
      )
    }
  }

  private implicit class NodeOps(node: NodeDef) {

    private lazy val FileTypes = Set((prov / "Entity").show)

    lazy val singleWordType: String = node.types match {
      case types if types contains (prov / "Activity").show   => "ProcessRun"
      case types if types contains (prov / "Collection").show => "Directory"
      case FileTypes                                          => "File"
    }
  }
}
