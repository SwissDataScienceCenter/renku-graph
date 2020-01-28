/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.acceptancetests.data._
import ch.datascience.graph.acceptancetests.flows.RdfStoreProvisioning.`data in the RDF store`
import ch.datascience.graph.acceptancetests.testing.AcceptanceTestPatience
import ch.datascience.graph.acceptancetests.tooling.GraphServices
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.model.EventsGenerators.projects
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.{FilePath, ProjectPath}
import ch.datascience.http.client.AccessToken
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.rdfstore.entities.bundles.exemplarLineageFlow.NodeDef
import io.circe.Json
import io.circe.literal._
import io.renku.jsonld.JsonLD
import org.http4s.Status._
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}
import sangria.ast.Document
import sangria.macros._

class LineageQuerySpec extends FeatureSpec with GivenWhenThen with GraphServices with AcceptanceTestPatience {

  private implicit val accessToken: AccessToken = accessTokens.generateOne
  private val project = projects.generateOne.copy(path = ProjectPath("namespace/lineage-project"))
  private val (jsons, examplarData) = exemplarLineageFlow(
    project.path,
    CommitId("0000012"),
    FilePath("figs/grid_plot.png")
  )
  import examplarData._

  feature("GraphQL query to find lineage") {

    scenario("As a user I would like to find project's lineage with a GraphQL query") {

      Given("some data in the RDF Store")
      `data in the RDF store`(
        project,
        commitId,
        JsonLD.arr(jsons: _*)
      )

      When("user posts a graphql query to fetch lineage")
      val response = knowledgeGraphClient POST lineageQuery

      Then("he should get OK response with project lineage in Json")
      response.status shouldBe Ok

      val lineageJson = response.bodyAsJson.hcursor.downField("data").downField("lineage")
      lineageJson.downField("edges").as[List[Json]].map(_.toSet) shouldBe theExpectedEdges
      lineageJson.downField("nodes").as[List[Json]].map(_.toSet) shouldBe theExpectedNodes
    }

    scenario("As a user I would like to find project's lineage with a named GraphQL query") {

      Given("some data in the RDF Store")

      When("user posts a graphql query to fetch lineage")
      val response = knowledgeGraphClient.POST(
        namedLineageQuery,
        variables = Map(
          "projectPath" -> project.path.toString,
          "commitId"    -> commitId.toString,
          "filePath"    -> filePath.toString
        )
      )

      Then("he should get OK response with project lineage in Json")
      response.status shouldBe Ok

      val lineageJson = response.bodyAsJson.hcursor.downField("data").downField("lineage")
      lineageJson.downField("edges").as[List[Json]].map(_.toSet) shouldBe theExpectedEdges
      lineageJson.downField("nodes").as[List[Json]].map(_.toSet) shouldBe theExpectedNodes
    }
  }

  private val lineageQuery: Document = graphql"""
    {
      lineage(projectPath: "namespace/lineage-project", commitId: "0000012", filePath: "figs/grid_plot.png") {
        nodes {
          id
          label
        }
        edges {
          source
          target
        }
      }
    }"""

  private val namedLineageQuery: Document = graphql"""
    query($$projectPath: ProjectPath!, $$commitId: CommitId!, $$filePath: FilePath!) { 
      lineage(projectPath: $$projectPath, commitId: $$commitId, filePath: $$filePath) { 
        nodes {
          id
          label
        }
        edges {
          source
          target
        }
      }
    }"""

  private lazy val theExpectedEdges = Right {
    Set(
      json"""{"source": ${`sha10 zhbikes`.toNodeId},            "target": ${`sha12 step2 renku update`.toNodeId}}""",
      json"""{"source": ${`sha7 plot_data`.toNodeId},           "target": ${`sha9 renku run`.toNodeId}}""",
      json"""{"source": ${`sha7 plot_data`.toNodeId},           "target": ${`sha12 step1 renku update`.toNodeId}}""",
      json"""{"source": ${`sha12 parquet`.toNodeId},            "target": ${`sha12 step1 renku update`.toNodeId}}""",
      json"""{"source": ${`sha12 step1 renku update`.toNodeId}, "target": ${`sha12 step2 grid_plot`.toNodeId}}""",
      json"""{"source": ${`sha7 clean_data`.toNodeId},          "target": ${`sha8 renku run`.toNodeId}}""",
      json"""{"source": ${`sha12 step2 renku update`.toNodeId}, "target": ${`sha12 parquet`.toNodeId}}""",
      json"""{"source": ${`sha7 clean_data`.toNodeId},          "target": ${`sha12 step2 renku update`.toNodeId}}"""
    )
  }

  private lazy val theExpectedNodes = Right {
    Set(
      json"""{"id": ${`sha7 clean_data`.toNodeId},        "label": ${`sha7 clean_data`.label}}""",
      json"""{"id": ${`sha7 plot_data`.toNodeId},      "label": ${`sha7 plot_data`.label}}""",
      json"""{"id": ${`sha8 renku run`.toNodeId},      "label": ${`sha8 renku run`.label}}""",
      json"""{"id": ${`sha9 renku run`.toNodeId},         "label": ${`sha9 renku run`.label}}""",
      json"""{"id": ${`sha10 zhbikes`.toNodeId}, "label": ${`sha10 zhbikes`.label}}""",
      json"""{"id": ${`sha12 step1 renku update`.toNodeId},         "label": ${`sha12 step1 renku update`.label}}""",
      json"""{"id": ${`sha12 step2 grid_plot`.toNodeId},      "label": ${`sha12 step2 grid_plot`.label}}""",
      json"""{"id": ${`sha12 step2 renku update`.toNodeId},      "label": ${`sha12 step2 renku update`.label}}""",
      json"""{"id": ${`sha12 parquet`.toNodeId},      "label": ${`sha12 parquet`.label}}"""
    )
  }

  private implicit class ResourceNameOps(node: NodeDef) {
    lazy val toNodeId: String = node.name.replace(fusekiBaseUrl.value, "")
  }
}
