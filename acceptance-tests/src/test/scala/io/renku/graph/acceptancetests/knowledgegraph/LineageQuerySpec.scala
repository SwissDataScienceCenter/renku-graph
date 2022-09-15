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
import io.circe.Json
import io.circe.literal._
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.fixed
import io.renku.graph.acceptancetests.data._
import io.renku.graph.acceptancetests.flows.TSProvisioning
import io.renku.graph.acceptancetests.tooling.GraphServices
import io.renku.graph.model
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.Schemas._
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.testentities.LineageExemplarData.ExemplarData
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.graph.model.testentities.{LineageExemplarData, NodeDef}
import io.renku.http.client.AccessToken
import io.renku.jsonld.syntax._
import org.http4s.Status._
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import sangria.ast.Document
import sangria.macros._

class LineageQuerySpec extends AnyFeatureSpec with GivenWhenThen with GraphServices with TSProvisioning {

  Feature("GraphQL query to find lineage") {
    val user = authUsers.generateOne
    val accessToken: AccessToken = user.accessToken

    val (exemplarData, project) = {
      val lineageData = LineageExemplarData(
        renkuProjectEntities(visibilityPublic)
          .map(
            _.copy(
              path = projects.Path("public/lineage-project"),
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
    Scenario("As a user I would like to find project's lineage with a GraphQL query") {

      Given("some data in the Triples Store")
      val commitId = commitIds.generateOne
      gitLabStub.setupProject(project, commitId)
      gitLabStub.addAuthenticated(user)
      mockCommitDataOnTripleGenerator(project, exemplarData.project.asJsonLD, commitId)
      `data in the Triples Store`(project, commitId, accessToken)

      When("user posts a graphql query to fetch lineage")
      val response = knowledgeGraphClient POST lineageQuery

      Then("he should get OK response with project lineage in Json")
      response.status shouldBe Ok

      val lineageJson = response.jsonBody.hcursor.downField("data").downField("lineage")
      lineageJson.downField("edges").as[List[Json]].map(_.toSet) shouldBe theExpectedEdges(exemplarData)
      lineageJson.downField("nodes").as[List[Json]].map(_.toSet) shouldBe theExpectedNodes(exemplarData)
    }

    Scenario("As a user I would like to find project's lineage with a named GraphQL query") {

      Given("some data in the Triples Store")

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

      val lineageJson = response.jsonBody.hcursor.downField("data").downField("lineage")
      lineageJson.downField("edges").as[List[Json]].map(_.toSet) shouldBe theExpectedEdges(exemplarData)
      lineageJson.downField("nodes").as[List[Json]].map(_.toSet) shouldBe theExpectedNodes(exemplarData)
    }
  }

  Feature("GraphQL query to find lineage with authentication") {
    val user = authUsers.generateOne
    implicit val accessToken: AccessToken = user.accessToken

    Scenario("As an authenticated user I would like to find lineage of project I am a member of with a GraphQL query") {

      val accessibleExemplarData = LineageExemplarData(
        renkuProjectEntities(fixed(Visibility.Private)).generateOne.copy(
          path = model.projects.Path("accessible/member-project"),
          members = Set(personEntities.generateOne.copy(maybeGitLabId = user.id.some))
        )
      )

      Given("some data in the Triples Store with a project I am a member of")
      val commitId = commitIds.generateOne
      val project  = dataProjects(accessibleExemplarData.project).generateOne
      gitLabStub.addAuthenticated(user)
      gitLabStub.setupProject(project, commitId)
      mockCommitDataOnTripleGenerator(project, accessibleExemplarData.project.asJsonLD, commitId)
      `data in the Triples Store`(project, commitId, accessToken)

      When("user posts a graphql query to fetch lineage of the project he is a member of")
      val response = knowledgeGraphClient.POST(
        namedLineageQuery,
        variables = Map(
          "projectPath" -> accessibleExemplarData.project.path.toString,
          "filePath"    -> accessibleExemplarData.`grid_plot entity`.location
        ),
        maybeAccessToken = accessToken.some
      )

      Then("he should get OK response with project lineage in Json")
      response.status shouldBe Ok

      val lineageJson = response.jsonBody.hcursor.downField("data").downField("lineage")
      lineageJson.downField("edges").as[List[Json]].map(_.toSet) shouldBe theExpectedEdges(accessibleExemplarData)
      lineageJson.downField("nodes").as[List[Json]].map(_.toSet) shouldBe theExpectedNodes(accessibleExemplarData)
    }

    Scenario("As an authenticated user I should not be able to find lineage of project I am not a member of") {
      val creator = authUsers.generateOne
      gitLabStub.addAuthenticated(creator)
      val privateExemplarData = LineageExemplarData(
        renkuProjectEntities(fixed(Visibility.Private)).generateOne.copy(
          path = model.projects.Path("private/secret-project"),
          members = Set.empty,
          maybeCreator = personEntities(creator.id.some).generateOne.some
        )
      )
      val commitId = commitIds.generateOne
      val project  = dataProjects(privateExemplarData.project).generateOne

      Given("I am authenticated")
      gitLabStub.addAuthenticated(user)
      gitLabStub.setupProject(project, commitId)
      mockCommitDataOnTripleGenerator(project, privateExemplarData.project.asJsonLD, commitId)
      `data in the Triples Store`(project, commitId, creator.accessToken)

      When("user posts a graphql query to fetch lineage of the project he is not a member of")
      val privateProjectResponse = knowledgeGraphClient.POST(
        namedLineageQuery,
        variables = Map(
          "projectPath" -> privateExemplarData.project.path.toString,
          "filePath"    -> privateExemplarData.`grid_plot entity`.location
        ),
        maybeAccessToken = accessToken.some
      )

      Then("he should get an OK response without lineage")
      privateProjectResponse.status shouldBe Ok

      privateProjectResponse.jsonBody.hcursor.downField("data").downField("lineage").as[Json] shouldBe Right(
        Json.Null
      )
    }

    Scenario("As an unauthenticated user I should not be able to find a lineage from a private project") {
      Given("some data in the Triples Store with a project I am a member of")
      val creator = authUsers.generateOne
      gitLabStub.addAuthenticated(creator)
      val exemplarData = LineageExemplarData(
        renkuProjectEntities(fixed(Visibility.Private)).generateOne.copy(
          path = model.projects.Path("unauthenticated/private-project"),
          maybeCreator = personEntities(creator.id.some).generateOne.some
        )
      )
      val commitId = commitIds.generateOne
      val project  = dataProjects(exemplarData.project).generateOne
      gitLabStub.setupProject(project, commitId)
      mockCommitDataOnTripleGenerator(project, exemplarData.project.asJsonLD, commitId)
      `data in the Triples Store`(project, commitId, creator.accessToken)

      When("user posts a graphql query to fetch lineage")
      val response = knowledgeGraphClient.POST(namedLineageQuery,
                                               variables = Map(
                                                 "projectPath" -> exemplarData.project.path.toString,
                                                 "filePath"    -> exemplarData.`grid_plot entity`.location
                                               )
      )

      Then("he should get a not found response without lineage")
      response.status                                                           shouldBe Ok
      response.jsonBody.hcursor.downField("data").downField("lineage").as[Json] shouldBe Right(Json.Null)
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
        json"""{"id": ${`activity3 node`.location},      "location": ${`activity3 node`.location},      "label": ${`activity3 node`.label},      "type": ${`activity3 node`.singleWordType}}""",
        json"""{"id": ${`clean_data entity`.location},   "location": ${`clean_data entity`.location},   "label": ${`clean_data entity`.label},   "type": ${`clean_data entity`.singleWordType}}""",
        json"""{"id": ${`bikesparquet entity`.location}, "location": ${`bikesparquet entity`.location}, "label": ${`bikesparquet entity`.label}, "type": ${`bikesparquet entity`.singleWordType}}""",
        json"""{"id": ${`plot_data entity`.location},    "location": ${`plot_data entity`.location},    "label": ${`plot_data entity`.label},    "type": ${`plot_data entity`.singleWordType}}""",
        json"""{"id": ${`activity4 node`.location},      "location": ${`activity4 node`.location},      "label": ${`activity4 node`.label},      "type": ${`activity4 node`.singleWordType}}""",
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
