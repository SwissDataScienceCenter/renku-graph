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
import ch.datascience.graph.acceptancetests.testing.AcceptanceTestPatience
import ch.datascience.graph.acceptancetests.tooling.GraphServices
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.model.GraphModelGenerators.{dataSetIds, dataSetNames}
import ch.datascience.graph.model.dataSets.{DataSetId, DataSetName}
import ch.datascience.graph.model.events.EventsGenerators.{projects, _}
import ch.datascience.graph.model.events.ProjectPath
import ch.datascience.rdfstore.RdfStoreData._
import flows.RdfStoreProvisioning._
import io.circe.Json
import io.circe.literal._
import org.http4s.Status._
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}
import sangria.ast.Document
import sangria.macros._

class DataSetsQuerySpec extends FeatureSpec with GivenWhenThen with GraphServices with AcceptanceTestPatience {

  private val project      = projects.generateOne.copy(path = ProjectPath("namespace/project"))
  private val commitId     = commitIds.generateOne
  private val dataSet1Id   = dataSetIds.generateOne
  private val dataSet1Name = dataSetNames.generateOne
  private val dataSet2Id   = dataSetIds.generateOne
  private val dataSet2Name = dataSetNames.generateOne

  feature("GraphQL query to find project's data-sets") {

    scenario("As a user I would like to find project's data-sets with a GraphQL query") {

      Given("some data in the RDF Store")
      val triples =
        singleFileAndCommitWithDataset(project.path, commitId, model.currentSchemaVersion, dataSet1Id, dataSet1Name) &+
          singleFileAndCommitWithDataset(project.path, commitId, model.currentSchemaVersion, dataSet2Id, dataSet2Name)
      `data in the RDF store`(project, commitId, triples)

      When("user posts a graphql query to fetch data-sets")
      val response = knowledgeGraphClient POST query

      Then("he should get OK response with project's data-sets in Json")
      response.status shouldBe Ok

      response.bodyAsJson.hcursor.downField("data").downField("dataSets").as[List[Json]].map(_.toSet) shouldBe Right {
        Set(
          json(dataSet1Id, dataSet1Name),
          json(dataSet2Id, dataSet2Name)
        )
      }
    }

    scenario("As a user I would like to find project's data-sets with a named GraphQL query") {

      Given("some data in the RDF Store")

      When("user posts a graphql query to fetch the data-sets")
      val response = knowledgeGraphClient.POST(
        namedQuery,
        variables = Map("projectPath" -> project.path.toString)
      )

      Then("he should get OK response with project lineage in Json")
      response.status shouldBe Ok

      response.bodyAsJson.hcursor.downField("data").downField("dataSets").as[List[Json]].map(_.toSet) shouldBe Right {
        Set(
          json(dataSet1Id, dataSet1Name),
          json(dataSet2Id, dataSet2Name)
        )
      }
    }
  }

  private val query: Document = graphql"""
    {
      dataSets(projectPath: "namespace/project") {
        id
        name
      }
    }"""

  private val namedQuery: Document = graphql"""
    query($$projectPath: ProjectPath!) { 
      dataSets(projectPath: $$projectPath) { 
        id
        name
      }
    }"""

  private def json(dataSetId: DataSetId, dataSetName: DataSetName) = json"""
    {
      "id": ${dataSetId.value}, 
      "name": ${dataSetName.value}
    }"""
}
