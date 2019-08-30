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
import ch.datascience.graph.acceptancetests.data._
import ch.datascience.graph.acceptancetests.flows.RdfStoreProvisioning.`data in the RDF store`
import ch.datascience.graph.acceptancetests.testing.AcceptanceTestPatience
import ch.datascience.graph.acceptancetests.tooling.GraphServices
import ch.datascience.graph.acceptancetests.tooling.RequestTools._
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.acceptancetests.tooling.TestReadabilityTools._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.dataSets.{DateCreated, Identifier}
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.http.rest.Links.{Href, Link, Rel, _links}
import ch.datascience.http.server.EndpointTester._
import ch.datascience.knowledgegraph.datasets.DataSetsGenerators._
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.rdfstore.RdfStoreData._
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import io.circe.Json
import io.circe.literal._
import org.http4s.Status._
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

import scala.util.Random

class DataSetsResourceSpec extends FeatureSpec with GivenWhenThen with GraphServices with AcceptanceTestPatience {

  private val project          = projects.generateOne
  private val dataSet1CommitId = commitIds.generateOne
  private val dataSet1 = dataSets.generateOne.copy(
    maybeDescription = Some(dataSetDescriptions.generateOne),
    published        = dataSetPublishingInfos.generateOne.copy(maybeDate = Some(dataSetPublishedDates.generateOne)),
    project          = List(DataSetProject(project.path))
  )
  private val dataSet2CommitId = commitIds.generateOne
  private val dataSet2 = dataSets.generateOne.copy(
    maybeDescription = None,
    published        = dataSetPublishingInfos.generateOne.copy(maybeDate = None),
    project          = List(DataSetProject(project.path))
  )

  feature("GET knowledge-graph/projects/<project-name>/data-sets to find project's data-sets") {

    scenario("As a user I would like to find project's data-sets by calling a REST enpoint") {

      Given("some data in the RDF Store")
      val triples = singleFileAndCommitWithDataset(
        project.path,
        dataSet1CommitId,
        dataSet1.created.agent.email,
        dataSet1.created.agent.name,
        dataSet1.id,
        dataSet1.name,
        dataSet1.maybeDescription,
        dataSet1.created.date,
        dataSet1.published.maybeDate,
        dataSet1.published.creators.map(creator => (creator.maybeEmail, creator.name)),
        dataSet1.part.map(part => (part.name, part.atLocation, part.dateCreated)),
        schemaVersion = currentSchemaVersion
      ) &+ singleFileAndCommitWithDataset(
        project.path,
        dataSet2CommitId,
        dataSet2.created.agent.email,
        dataSet2.created.agent.name,
        dataSet2.id,
        dataSet2.name,
        dataSet2.maybeDescription,
        dataSet2.created.date,
        dataSet2.published.maybeDate,
        dataSet2.published.creators.map(creator => (creator.maybeEmail, creator.name)),
        dataSet2.part.map(part => (part.name, part.atLocation, part.dateCreated)),
        schemaVersion = currentSchemaVersion
      )

      `data in the RDF store`(project, dataSet1CommitId, triples)

      When("user fetches project's data-sets with GET knowledge-graph/projects/<project-name>/data-sets")
      val projectDataSetsResponse = knowledgeGraphClient GET s"knowledge-graph/projects/${project.path.asUrlEncoded}/data-sets"

      Then("he should get OK response with project's data-sets")
      projectDataSetsResponse.status shouldBe Ok
      val Right(foundDataSets) = projectDataSetsResponse.bodyAsJson.as[List[Json]]
      foundDataSets should contain theSameElementsAs List(shortJson(dataSet1), shortJson(dataSet2))

      When("user then fetches details of the chosen data-set with the link from the response")
      val someDataSetDetailsLink =
        Random
          .shuffle(foundDataSets)
          .headOption
          .flatMap(_._links.get(Rel("details")))
          .getOrFail(message = "No link with rel 'details'")
      val dataSetDetailsResponse = restClient GET someDataSetDetailsLink.toString

      Then("he should get OK response with data-set details")
      dataSetDetailsResponse.status shouldBe Ok
      val Right(foundDataSetDetails) = dataSetDetailsResponse.bodyAsJson.as[Json]
      val expectedDataSet = List(dataSet1, dataSet2)
        .find(dataSet => someDataSetDetailsLink.value contains dataSet.id.value)
        .getOrFail(message = "Returned 'details' link does not point to any data-set in the RDF store")

      foundDataSetDetails.hcursor.downField("identifier").as[Identifier] shouldBe Right(expectedDataSet.id)
      foundDataSetDetails.hcursor.downField("created").downField("dateCreated").as[DateCreated] shouldBe Right(
        expectedDataSet.created.date
      )
    }
  }

  private def shortJson(dataSet: DataSet) =
    json"""
    {
      "identifier": ${dataSet.id.value}, 
      "name": ${dataSet.name.value}
    }""".deepMerge {
      _links(
        Link(Rel("details"), Href(renkuResourceUrl / "data-sets" / dataSet.id.value))
      )
    }
}
