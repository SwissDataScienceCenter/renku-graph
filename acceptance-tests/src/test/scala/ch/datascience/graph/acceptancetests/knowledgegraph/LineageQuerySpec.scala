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

package ch.datascience.graph.acceptancetests.knowledgegraph

import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators.{accessTokens, authUsers}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.acceptancetests.data._
import ch.datascience.graph.acceptancetests.flows.RdfStoreProvisioning.`data in the RDF store`
import ch.datascience.graph.acceptancetests.stubs.GitLab.`GET <gitlabApi>/user returning OK`
import ch.datascience.graph.acceptancetests.testing.AcceptanceTestPatience
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.acceptancetests.tooling.{GraphServices, ModelImplicits}
import ch.datascience.graph.model
import ch.datascience.graph.model.{projects, testentities}
import ch.datascience.graph.model.testentities.LineageExemplarData.ExemplarData
import ch.datascience.graph.model.testentities.Project.ForksCount
import ch.datascience.graph.model.testentities.{LineageExemplarData, NodeDef, gitLabApiUrl => _, renkuBaseUrl => _, _}
import ch.datascience.http.client.AccessToken
import io.circe.Json
import io.circe.literal._
import io.renku.jsonld.JsonLD
import org.http4s.Status._
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should
import sangria.ast.Document
import sangria.macros._

class LineageQuerySpec
    extends AnyFeatureSpec
    with GivenWhenThen
    with GraphServices
    with AcceptanceTestPatience
    with should.Matchers
    with ModelImplicits {

  Feature("GraphQL query to find lineage") {

    implicit val accessToken: AccessToken = accessTokens.generateOne
    val project = dataProjects(
      projectEntities[testentities.Project.ForksCount.Zero](visibilityPublic)
        .map(
          _.copy(
            path = projects.Path("public/lineage-project"),
            agent = cliVersion
          )
        )
    ).generateOne

    val (jsons, exemplarData) = LineageExemplarData(project.entitiesProject)

    /**  ========================================== EXPECTED GRAPH  ==========================================
      *  When looking for grid_plot of commit 9
      *                                                   sha7 plot_data +--------------+
      *                                                                                 |
      *                                                                                 v                   ------------
      * sha3 zhbikes+---------------> sha8 renku run +------->bikesParquet +------->sha9 renku run+------> | grid_plot |
      *                                       ^                                                            ------------
      *                                       |
      * sha7 clean_data +--------------------+
      */

    Scenario("As a user I would like to find project's lineage with a GraphQL query") {

      Given("some data in the RDF Store")
      `data in the RDF store`(project, JsonLD.arr(jsons: _*))

      When("user posts a graphql query to fetch lineage")
      val response = knowledgeGraphClient POST lineageQuery

      Then("he should get OK response with project lineage in Json")
      response.status shouldBe Ok

      val lineageJson = response.bodyAsJson.hcursor.downField("data").downField("lineage")
      lineageJson.downField("edges").as[List[Json]].map(_.toSet) shouldBe theExpectedEdges(exemplarData)
      lineageJson.downField("nodes").as[List[Json]].map(_.toSet) shouldBe theExpectedNodes(exemplarData)
    }

    Scenario("As a user I would like to find project's lineage with a named GraphQL query") {

      Given("some data in the RDF Store")

      When("user posts a graphql query to fetch lineage")
      val response = knowledgeGraphClient.POST(
        namedLineageQuery,
        variables = Map(
          "projectPath" -> project.path.toString,
          "filePath"    -> exemplarData.`grid_plot entity`.location
        )
      )

      Then("he should get OK response with project lineage in Json")
      response.status shouldBe Ok

      val lineageJson = response.bodyAsJson.hcursor.downField("data").downField("lineage")
      lineageJson.downField("edges").as[List[Json]].map(_.toSet) shouldBe theExpectedEdges(exemplarData)
      lineageJson.downField("nodes").as[List[Json]].map(_.toSet) shouldBe theExpectedNodes(exemplarData)
    }
  }

  Feature("GraphQL query to find lineage with authentication") {
    val user = authUsers.generateOne
    implicit val accessToken: AccessToken = user.accessToken
    Scenario("As an authenticated user I would like to find lineage of project I am a member of with a GraphQL query") {

      val accessibleProject = projectEntities[ForksCount.Zero](visibilityNonPublic).generateOne.copy(
        path = model.projects.Path("accessible/member-project"),
        members = Set(personEntities.generateOne.copy(maybeGitLabId = user.id.some))
      )

      val (accessibleJsons, accessibleExemplarData) = LineageExemplarData(accessibleProject)

      Given("some data in the RDF Store with a project I am a member of")
      `data in the RDF store`(dataProjects(accessibleProject).generateOne, JsonLD.arr(accessibleJsons: _*))

      And("a project I am not a member of")
      val privateProject =
        projectEntities[testentities.Project.ForksCount.Zero](visibilityNonPublic).generateOne.copy(
          path = model.projects.Path("private/secret-project")
        )

      val (privateJsons, privateExemplarData) = LineageExemplarData(privateProject)
      `data in the RDF store`(dataProjects(privateProject).generateOne, JsonLD.arr(privateJsons: _*))

      And("I am authenticated")
      `GET <gitlabApi>/user returning OK`(user)

      When("user posts a graphql query to fetch lineage of the project he is a member of")
      val response =
        knowledgeGraphClient.POST(
          namedLineageQuery,
          variables = Map(
            "projectPath" -> accessibleProject.path.toString,
            "filePath"    -> accessibleExemplarData.`grid_plot entity`.location
          ),
          maybeAccessToken = accessToken.some
        )

      Then("he should get OK response with project lineage in Json")
      response.status shouldBe Ok

      val lineageJson = response.bodyAsJson.hcursor.downField("data").downField("lineage")
      lineageJson.downField("edges").as[List[Json]].map(_.toSet) shouldBe theExpectedEdges(accessibleExemplarData)
      lineageJson.downField("nodes").as[List[Json]].map(_.toSet) shouldBe theExpectedNodes(accessibleExemplarData)

      When("user posts a graphql query to fetch lineage of the project he is not a member of")
      val privateProjectResponse =
        knowledgeGraphClient.POST(
          namedLineageQuery,
          variables = Map(
            "projectPath" -> privateProject.path.toString,
            "filePath"    -> privateExemplarData.`grid_plot entity`.location
          ),
          maybeAccessToken = accessToken.some
        )

      Then("he should get an OK response without lineage")
      privateProjectResponse.status shouldBe Ok

      privateProjectResponse.bodyAsJson.hcursor.downField("data").downField("lineage").as[Json] shouldBe Right(
        Json.Null
      )
    }

    Scenario("As an unauthenticated user I should not be able to find a lineage from a private project") {
      Given("some data in the RDF Store with a project I am a member of")
      val privateProject = projectEntities[ForksCount.Zero](visibilityNonPublic).generateOne.copy(
        path = model.projects.Path("unauthenticated/private-project")
      )

      val (privateJsons, examplarData) = LineageExemplarData(privateProject)
      `data in the RDF store`(dataProjects(privateProject).generateOne, JsonLD.arr(privateJsons: _*))

      When("user posts a graphql query to fetch lineage")
      val response = knowledgeGraphClient.POST(namedLineageQuery,
                                               variables = Map(
                                                 "projectPath" -> privateProject.path.toString,
                                                 "filePath"    -> examplarData.`grid_plot entity`.location
                                               )
      )

      Then("he should get a not found response without lineage")
      response.status                                                             shouldBe Ok
      response.bodyAsJson.hcursor.downField("data").downField("lineage").as[Json] shouldBe Right(Json.Null)
    }
  }

  private lazy val lineageQuery: Document = graphql"""{
      lineage(projectPath: "public/lineage-project", filePath: "figs/grid_plot.png") {
        nodes {
          id
          location
          label
          type
        }
        edges {
          source
          target
        }
      }
    }"""

  private lazy val namedLineageQuery: Document = graphql"""
    query($$projectPath: ProjectPath!, $$filePath: FilePath!) { 
      lineage(projectPath: $$projectPath, filePath: $$filePath) { 
        nodes {
          id
          location
          label
          type
        }
        edges {
          source
          target
        }
      }
    }"""

  private def theExpectedEdges(exemplarData: ExemplarData) = {
    import exemplarData._
    Right {
      Set(
        json"""{"source": ${`zhbikes folder`.location},     "target": ${`activity3 plan1`.location}}""",
        json"""{"source": ${`clean_data entity`.location},  "target": ${`activity3 plan1`.location}}""",
        json"""{"source": ${`activity3 plan1`.location},    "target": ${`bikesparquet entity`.location}}""",
        json"""{"source": ${`bikesparquet entity`.location},"target": ${`activity4 plan2`.location}}""",
        json"""{"source": ${`plot_data entity`.location},   "target": ${`activity4 plan2`.location}}""",
        json"""{"source": ${`activity4 plan2`.location},    "target": ${`grid_plot entity`.location}}"""
      )
    }
  }

  private def theExpectedNodes(exemplarData: ExemplarData) = {
    import exemplarData._
    Right {
      Set(
        json"""{"id": ${`zhbikes folder`.location},      "location": ${`zhbikes folder`.location},      "label": ${`zhbikes folder`.label},      "type": ${`zhbikes folder`.singleWordType}   }""",
        json"""{"id": ${`activity3 plan1`.location},     "location": ${`activity3 plan1`.location},     "label": ${`activity3 plan1`.label},     "type": ${`activity3 plan1`.singleWordType}}""",
        json"""{"id": ${`clean_data entity`.location},   "location": ${`clean_data entity`.location},   "label": ${`clean_data entity`.label},   "type": ${`clean_data entity`.singleWordType}}""",
        json"""{"id": ${`bikesparquet entity`.location}, "location": ${`bikesparquet entity`.location}, "label": ${`bikesparquet entity`.label}, "type": ${`bikesparquet entity`.singleWordType} }""",
        json"""{"id": ${`plot_data entity`.location},    "location": ${`plot_data entity`.location},    "label": ${`plot_data entity`.label},    "type": ${`plot_data entity`.singleWordType} }""",
        json"""{"id": ${`activity4 plan2`.location},     "location": ${`activity4 plan2`.location},     "label": ${`activity4 plan2`.label},     "type": ${`activity4 plan2`.singleWordType} }""",
        json"""{"id": ${`grid_plot entity`.location},    "location": ${`grid_plot entity`.location},    "label": ${`grid_plot entity`.label},    "type": ${`grid_plot entity`.singleWordType} }"""
      )
    }
  }

  private implicit class NodeOps(node: NodeDef) {

    private lazy val FileTypes = Set((prov / "Entity").show, (wfprov / "Artifact").show)

    lazy val singleWordType: String = node.types match {
      case types if types contains (prov / "Activity").show   => "ProcessRun"
      case types if types contains (prov / "Collection").show => "Directory"
      case FileTypes                                          => "File"
    }
  }
}
