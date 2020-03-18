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
import ch.datascience.graph.model
import ch.datascience.http.client.AccessToken
import ch.datascience.knowledgegraph.projects.ProjectsGenerators.projects
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

import scala.collection.Set

class LineageQuerySpec extends FeatureSpec with GivenWhenThen with GraphServices with AcceptanceTestPatience {

  private implicit val accessToken: AccessToken = accessTokens.generateOne
  private val project               = projects.generateOne.copy(path = model.projects.Path("namespace/lineage-project"))
  private val (jsons, examplarData) = exemplarLineageFlow(project.path)
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
      lineage(projectPath: "namespace/lineage-project", filePath: "figs/grid_plot.png") {
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

  private val namedLineageQuery: Document = graphql"""
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

  private lazy val theExpectedEdges = Right {
    Set(
      json"""{"source": ${`sha3 zhbikes`.location},             "target": ${`sha8 renku run`.location}}""",
      json"""{"source": ${`sha7 plot_data`.location},           "target": ${`sha9 renku run`.location}}""",
      json"""{"source": ${`sha7 clean_data`.location},          "target": ${`sha8 renku run`.location}}""",
      json"""{"source": ${`sha8 renku run`.location},           "target": ${`sha8 parquet`.location}}""",
      json"""{"source": ${`sha8 parquet`.location},             "target": ${`sha9 renku run`.location}}""",
      json"""{"source": ${`sha9 renku run`.location},           "target": ${`sha9 plot_data`.location}}"""
    )
  }

  private lazy val theExpectedNodes = Right {
    Set(
      json"""{"id": ${`sha3 zhbikes`.location},             "location": ${`sha3 zhbikes`.location},             "label": ${`sha3 zhbikes`.label},             "type": ${`sha3 zhbikes`.singleWordType}   }""",
      json"""{"id": ${`sha7 clean_data`.location},          "location": ${`sha7 clean_data`.location},          "label": ${`sha7 clean_data`.label},          "type": ${`sha7 clean_data`.singleWordType}}""",
      json"""{"id": ${`sha7 plot_data`.location},           "location": ${`sha7 plot_data`.location},           "label": ${`sha7 plot_data`.label},           "type": ${`sha7 plot_data`.singleWordType} }""",
      json"""{"id": ${`sha8 renku run`.location},           "location": ${`sha8 renku run`.location},           "label": ${`sha8 renku run`.label},           "type": ${`sha8 renku run`.singleWordType} }""",
      json"""{"id": ${`sha8 parquet`.location},             "location": ${`sha8 parquet`.location},             "label": ${`sha8 parquet`.label},             "type": ${`sha8 parquet`.singleWordType}   }""",
      json"""{"id": ${`sha9 renku run`.location},           "location": ${`sha9 renku run`.location},           "label": ${`sha9 renku run`.label},           "type": ${`sha9 renku run`.singleWordType} }""",
      json"""{"id": ${`sha9 plot_data`.location},           "location": ${`sha9 plot_data`.location},           "label": ${`sha9 plot_data`.label},           "type": ${`sha9 plot_data`.singleWordType} }"""
    )
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
