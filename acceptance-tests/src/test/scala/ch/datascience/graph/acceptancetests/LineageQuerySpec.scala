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

package ch.datascience.graph.acceptancetests

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.acceptancetests.data.KnowledgeGraph._
import ch.datascience.graph.acceptancetests.tooling.GraphServices
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.model.events.EventsGenerators.{commitIds, projects}
import ch.datascience.graph.model.events.ProjectPath
import ch.datascience.knowledgegraph.graphql
import ch.datascience.tinytypes.json.TinyTypeEncoders._
import flows.RdfStoreProvisioning._
import graphql.lineage.TestData._
import io.circe.Json
import io.circe.literal._
import org.http4s.Status._
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}
import sangria.ast.Document
import sangria.macros._

class LineageQuerySpec extends FeatureSpec with GivenWhenThen with GraphServices {

  feature("GraphQL query to find lineage") {

    scenario("As a user I would like to find project's lineage with a GraphQL query") {

      val project  = projects.generateOne.copy(path = ProjectPath("namespace/project"))
      val commitId = commitIds.generateOne

      Given("some data in the RDF Store")
      `data in the RDF store`(project, commitId, triples(project.path))

      When("user posts a graphql query to fetch lineage")
      val response = graphServiceClient POST lineageQuery

      Then("he should get OK response with project lineage in Json")
      response.status shouldBe Ok

      val lineageJson = response.bodyAsJson.hcursor.downField("data").downField("lineage")
      lineageJson.downField("edges").as[List[Json]].map(_.toSet) shouldBe Right(
        Set(
          json"""{"source": ${`commit1-input-data`},        "target": ${`commit3-renku-run`}}""",
          json"""{"source": ${`commit2-source-file1`},      "target": ${`commit3-renku-run`}}""",
          json"""{"source": ${`commit3-renku-run`},         "target": ${`commit3-preprocessed-data`}}""",
          json"""{"source": ${`commit3-preprocessed-data`}, "target": ${`commit4-renku-run`}}""",
          json"""{"source": ${`commit2-source-file2`},      "target": ${`commit4-renku-run`}}""",
          json"""{"source": ${`commit4-renku-run`},         "target": ${`commit4-result-file1`}}""",
          json"""{"source": ${`commit4-renku-run`},         "target": ${`commit4-result-file2`}}"""
        )
      )
      lineageJson.downField("nodes").as[List[Json]].map(_.toSet) shouldBe Right(
        Set(
          json"""{"id": ${`commit1-input-data`.id},        "label": ${`commit1-input-data`.label}}""",
          json"""{"id": ${`commit2-source-file1`.id},      "label": ${`commit2-source-file1`.label}}""",
          json"""{"id": ${`commit2-source-file2`.id},      "label": ${`commit2-source-file2`.label}}""",
          json"""{"id": ${`commit3-renku-run`.id},         "label": ${`commit3-renku-run`.label}}""",
          json"""{"id": ${`commit3-preprocessed-data`.id}, "label": ${`commit3-preprocessed-data`.label}}""",
          json"""{"id": ${`commit4-renku-run`.id},         "label": ${`commit4-renku-run`.label}}""",
          json"""{"id": ${`commit4-result-file1`.id},      "label": ${`commit4-result-file1`.label}}""",
          json"""{"id": ${`commit4-result-file2`.id},      "label": ${`commit4-result-file2`.label}}"""
        )
      )
    }
  }

  private val lineageQuery: Document = graphql"""
    {
      lineage(projectPath: "namespace/project") {
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
}
