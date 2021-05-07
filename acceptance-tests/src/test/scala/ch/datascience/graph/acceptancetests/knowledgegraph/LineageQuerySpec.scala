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
import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.acceptancetests.data._
import ch.datascience.graph.acceptancetests.flows.RdfStoreProvisioning.`data in the RDF store`
import ch.datascience.graph.acceptancetests.stubs.GitLab.`GET <gitlabApi>/user returning OK`
import ch.datascience.graph.acceptancetests.testing.AcceptanceTestPatience
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.acceptancetests.tooling.{GraphServices, ModelImplicits}
import ch.datascience.graph.model
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.GraphModelGenerators.authUsers
import ch.datascience.graph.model.projects.Visibility
import ch.datascience.http.client.AccessToken
import ch.datascience.knowledgegraph.projects.ProjectsGenerators.projects
import ch.datascience.rdfstore.entities.EntitiesGenerators.persons
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.rdfstore.entities.bundles.exemplarLineageFlow.ExemplarData
import io.circe.Json
import io.circe.literal._
import io.renku.jsonld.JsonLD
import org.http4s.Status._
import org.scalacheck.Gen
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should
import sangria.ast.Document
import sangria.macros._

import scala.collection.Set

class LineageQuerySpec
    extends AnyFeatureSpec
    with GivenWhenThen
    with GraphServices
    with AcceptanceTestPatience
    with should.Matchers
    with ModelImplicits {

  /**  ========================================== ORIGINAL GRAPH  ======================================================
    *                                                   sha7 plot_data +----------------+
    *                                                                                   |
    *                                                                                   v
    * sha3 zhbikes+---------------> sha8 renku run +------->bikesParquet +------------>sha9 renku run+------> grid_plot
    *                                       ^                                                +
    *                                       |                                                |
    * sha7 clean_data +--------------------+                                                 +-----------> cumulative
    */

  Feature("GraphQL query to find lineage") {
    implicit val accessToken: AccessToken = accessTokens.generateOne
    val project =
      projects.generateOne.copy(path = model.projects.Path("public/lineage-project"), visibility = Visibility.Public)

    val (jsons, exemplarData) = exemplarLineageFlow(project.path)

    /**  ========================================== EXPECTED GRAPH  ====================================================
      *  When looking for figs/grid_plot of commit 9
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
      `data in the RDF store`(
        project,
        exemplarData.commitId,
        exemplarData.committer,
        JsonLD.arr(jsons: _*)
      )()

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
          "filePath"    -> exemplarData.location.toString
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

      val accessibleProject = projects.generateOne.copy(path = model.projects.Path("accessible/member-project"),
                                                        visibility =
                                                          Gen.oneOf(Visibility.Private, Visibility.Internal).generateOne
      )

      val (accessibleJsons, accessibleExemplarData) = exemplarLineageFlow(accessibleProject.path)

      Given("some data in the RDF Store with a project I am a member of")
      `data in the RDF store`(
        accessibleProject,
        accessibleExemplarData.commitId,
        accessibleExemplarData.committer,
        JsonLD.arr(accessibleJsons: _*)
      )(members = persons.generateOne.copy(maybeGitLabId = user.id.some).asMembersList())

      And("a project I am not a member of")
      val privateProject = projects.generateOne.copy(path = model.projects.Path("private/secret-project"),
                                                     visibility =
                                                       Gen.oneOf(Visibility.Private, Visibility.Internal).generateOne
      )

      val (privateJsons, privateExemplarData) = exemplarLineageFlow(privateProject.path)
      `data in the RDF store`(
        privateProject,
        privateExemplarData.commitId,
        privateExemplarData.committer,
        JsonLD.arr(privateJsons: _*)
      )()

      And("I am authenticated")
      `GET <gitlabApi>/user returning OK`(user)

      When("user posts a graphql query to fetch lineage of the project he is a member of")
      val response =
        knowledgeGraphClient.POST(
          namedLineageQuery,
          variables = Map(
            "projectPath" -> accessibleProject.path.toString,
            "filePath"    -> accessibleExemplarData.location.toString
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
            "filePath"    -> privateExemplarData.location.toString
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
      val privateProject = projects.generateOne.copy(path = model.projects.Path("unauthenticated/private-project"),
                                                     visibility =
                                                       Gen.oneOf(Visibility.Private, Visibility.Internal).generateOne
      )

      val (privateJsons, examplarData) = exemplarLineageFlow(privateProject.path)
      `data in the RDF store`(
        privateProject,
        commitIds.generateOne,
        persons.generateOne,
        JsonLD.arr(privateJsons: _*)
      )()

      When("user posts a graphql query to fetch lineage")
      val response = knowledgeGraphClient.POST(namedLineageQuery,
                                               variables = Map(
                                                 "projectPath" -> privateProject.path.toString,
                                                 "filePath"    -> examplarData.location.toString
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

  def theExpectedEdges(exemplarData: ExemplarData) = {
    import exemplarData._
    Right {
      Set(
        json"""{"source": ${`sha3 zhbikes`.location},    "target": ${`sha8 renku run`.location}}""",
        json"""{"source": ${`sha7 plot_data`.location},  "target": ${`sha9 renku run`.location}}""",
        json"""{"source": ${`sha7 clean_data`.location}, "target": ${`sha8 renku run`.location}}""",
        json"""{"source": ${`sha8 renku run`.location},  "target": ${`sha8 parquet`.location}}""",
        json"""{"source": ${`sha8 parquet`.location},    "target": ${`sha9 renku run`.location}}""",
        json"""{"source": ${`sha9 renku run`.location},  "target": ${`sha9 grid_plot`.location}}"""
      )
    }
  }

  def theExpectedNodes(exemplarData: ExemplarData) = {
    import exemplarData._
    Right {
      Set(
        json"""{"id": ${`sha3 zhbikes`.location},    "location": ${`sha3 zhbikes`.location},    "label": ${`sha3 zhbikes`.label},    "type": ${`sha3 zhbikes`.singleWordType}   }""",
        json"""{"id": ${`sha7 clean_data`.location}, "location": ${`sha7 clean_data`.location}, "label": ${`sha7 clean_data`.label}, "type": ${`sha7 clean_data`.singleWordType}}""",
        json"""{"id": ${`sha7 plot_data`.location},  "location": ${`sha7 plot_data`.location},  "label": ${`sha7 plot_data`.label},  "type": ${`sha7 plot_data`.singleWordType} }""",
        json"""{"id": ${`sha8 renku run`.location},  "location": ${`sha8 renku run`.location},  "label": ${`sha8 renku run`.label},  "type": ${`sha8 renku run`.singleWordType} }""",
        json"""{"id": ${`sha8 parquet`.location},    "location": ${`sha8 parquet`.location},    "label": ${`sha8 parquet`.label},    "type": ${`sha8 parquet`.singleWordType}   }""",
        json"""{"id": ${`sha9 renku run`.location},  "location": ${`sha9 renku run`.location},  "label": ${`sha9 renku run`.label},  "type": ${`sha9 renku run`.singleWordType} }""",
        json"""{"id": ${`sha9 grid_plot`.location},  "location": ${`sha9 grid_plot`.location},  "label": ${`sha9 grid_plot`.label},  "type": ${`sha9 grid_plot`.singleWordType} }"""
      )
    }
  }

  private implicit class NodeOps(node: NodeDef) {

    private lazy val FileTypes = Set("http://www.w3.org/ns/prov#Entity", "http://purl.org/wf4ever/wfprov#Artifact")

    lazy val singleWordType: String = node.types match {
      case types if types contains "http://purl.org/wf4ever/wfprov#ProcessRun" => "ProcessRun"
      case types if types contains "http://www.w3.org/ns/prov#Collection"      => "Directory"
      case FileTypes                                                           => "File"
    }
  }
}
