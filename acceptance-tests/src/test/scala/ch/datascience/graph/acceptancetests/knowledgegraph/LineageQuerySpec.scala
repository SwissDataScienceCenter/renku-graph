/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.acceptancetests.flows.RdfStoreProvisioning.`data in the RDF store`
import ch.datascience.graph.acceptancetests.testing.AcceptanceTestPatience
import ch.datascience.graph.acceptancetests.tooling.GraphServices
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.events.EventsGenerators.projects
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.rdfstore.RdfStoreData
import ch.datascience.rdfstore.RdfStoreData.MultiFileAndCommitTriples._
import ch.datascience.tinytypes.json.TinyTypeEncoders._
import io.circe.literal._
import io.circe.{Encoder, Json}
import org.http4s.Status._
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}
import sangria.ast.Document
import sangria.macros._

class LineageQuerySpec extends FeatureSpec with GivenWhenThen with GraphServices with AcceptanceTestPatience {

  private val project  = projects.generateOne.copy(path = ProjectPath("namespace/project"))
  private val commitId = CommitId("0000001")
  private val testData = RdfStoreData.multiFileAndCommit(project.path)

  feature("GraphQL query to find lineage") {

    scenario("As a user I would like to find project's lineage with a GraphQL query") {

      Given("some data in the RDF Store")
      `data in the RDF store`(project, commitId, testData.triples)

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
          "projectPath" -> "namespace/project",
          "commitId"    -> "0000004",
          "filePath"    -> "result-file-1"
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
      lineage(projectPath: "namespace/project", commitId: "0000004", filePath: "result-file-1") {
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
    import testData._
    Set(
      json"""{"source": ${`commit1-input-data`.name},        "target": ${`commit3-renku-run`.name}}""",
      json"""{"source": ${`commit2-source-file1`.name},      "target": ${`commit3-renku-run`.name}}""",
      json"""{"source": ${`commit3-renku-run`.name},         "target": ${`commit3-preprocessed-data`.name}}""",
      json"""{"source": ${`commit3-preprocessed-data`.name}, "target": ${`commit4-renku-run`.name}}""",
      json"""{"source": ${`commit2-source-file2`.name},      "target": ${`commit4-renku-run`.name}}""",
      json"""{"source": ${`commit4-renku-run`.name},         "target": ${`commit4-result-file1`.name}}"""
    )
  }

  private lazy val theExpectedNodes = Right {
    import testData._
    Set(
      json"""{"id": ${`commit1-input-data`.name},        "label": ${`commit1-input-data`.label}}""",
      json"""{"id": ${`commit2-source-file1`.name},      "label": ${`commit2-source-file1`.label}}""",
      json"""{"id": ${`commit2-source-file2`.name},      "label": ${`commit2-source-file2`.label}}""",
      json"""{"id": ${`commit3-renku-run`.name},         "label": ${`commit3-renku-run`.label}}""",
      json"""{"id": ${`commit3-preprocessed-data`.name}, "label": ${`commit3-preprocessed-data`.label}}""",
      json"""{"id": ${`commit4-renku-run`.name},         "label": ${`commit4-renku-run`.label}}""",
      json"""{"id": ${`commit4-result-file1`.name},      "label": ${`commit4-result-file1`.label}}"""
    )
  }

  private implicit val resourceNameEncoder:  Encoder[ResourceName]  = stringEncoder[ResourceName]
  private implicit val ResourceLabelEncoder: Encoder[ResourceLabel] = stringEncoder[ResourceLabel]
}
