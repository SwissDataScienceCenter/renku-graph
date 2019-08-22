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
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.graph.model.events.ProjectPath
import ch.datascience.knowledgegraph.graphql.datasets.DataSetsGenerators._
import ch.datascience.knowledgegraph.graphql.datasets.model.DataSet
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

  private val project  = projects.generateOne.copy(path = ProjectPath("namespace/project"))
  private val commitId = commitIds.generateOne
  private val dataSet1 = dataSets.generateOne.copy(
    maybeDescription = Some(dataSetDescriptions.generateOne),
    maybePublished   = Some(dataSetPublishingInfos.generateOne)
  )
  private val dataSet2 = dataSets.generateOne.copy(
    maybeDescription = None,
    maybePublished   = None
  )

  feature("GraphQL query to find project's data-sets") {

    scenario("As a user I would like to find project's data-sets with a GraphQL query") {

      Given("some data in the RDF Store")
      val triples = singleFileAndCommitWithDataset(
        project.path,
        commitId,
        model.currentSchemaVersion,
        dataSet1.id,
        dataSet1.name,
        dataSet1.maybeDescription,
        dataSet1.created.date,
        dataSet1.created.creator.email,
        dataSet1.created.creator.name,
        dataSet1.maybePublished.map(_.date)
      ) &+ singleFileAndCommitWithDataset(
        project.path,
        commitId,
        model.currentSchemaVersion,
        dataSet2.id,
        dataSet2.name,
        dataSet2.maybeDescription,
        dataSet2.created.date,
        dataSet2.created.creator.email,
        dataSet2.created.creator.name,
        dataSet2.maybePublished.map(_.date)
      )

      `data in the RDF store`(project, commitId, triples)

      When("user posts a graphql query to fetch data-sets")
      val response = knowledgeGraphClient POST query

      Then("he should get OK response with project's data-sets in Json")
      response.status shouldBe Ok

      val Right(responseJson) = response.bodyAsJson.hcursor.downField("data").downField("dataSets").as[List[Json]]

      responseJson should contain theSameElementsAs List(json(dataSet1), json(dataSet2))
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

      val Right(responseJson) = response.bodyAsJson.hcursor.downField("data").downField("dataSets").as[List[Json]]

      responseJson should contain theSameElementsAs List(json(dataSet1), json(dataSet2))
    }
  }

  private val query: Document = graphql"""
    {
      dataSets(projectPath: "namespace/project") {
        identifier
        name
        description
        created { dateCreated creator { email name } }
        published { datePublished }
      }
    }"""

  private val namedQuery: Document = graphql"""
    query($$projectPath: ProjectPath!) { 
      dataSets(projectPath: $$projectPath) { 
        identifier
        name
        description
        created { dateCreated creator { email name } }
        published { datePublished }
      }
    }"""

  // format: off
  private def json(dataSet: DataSet) = json"""
    {
      "identifier": ${dataSet.id.value}, 
      "name": ${dataSet.name.value},
      "description": ${dataSet.maybeDescription.map(_.value).map(Json.fromString).getOrElse(Json.Null)},
      "created": {
        "dateCreated": ${dataSet.created.date.value},
        "creator": {
          "email": ${dataSet.created.creator.email.value},
          "name": ${dataSet.created.creator.name.value}
        }
      },
      "published": ${dataSet.maybePublished.map { published => json"""{
        "datePublished": ${published.date.value}
      }"""}.getOrElse(Json.Null)}
    }"""
  // format: on
}
