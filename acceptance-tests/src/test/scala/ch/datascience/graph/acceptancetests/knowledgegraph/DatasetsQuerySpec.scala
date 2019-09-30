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

import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.acceptancetests.data._
import ch.datascience.graph.acceptancetests.flows.RdfStoreProvisioning.`data in the RDF store`
import ch.datascience.graph.acceptancetests.testing.AcceptanceTestPatience
import ch.datascience.graph.acceptancetests.tooling.GraphServices
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.rdfstore.triples._
import ch.datascience.tinytypes.json.TinyTypeEncoders._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.http4s.Status._
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}
import sangria.ast.Document
import sangria.macros._

class DatasetsQuerySpec extends FeatureSpec with GivenWhenThen with GraphServices with AcceptanceTestPatience {

  private val project          = projects.generateOne.copy(path = ProjectPath("namespace/datasets-project"))
  private val projectName      = projectNames.generateOne
  private val dataset1CommitId = commitIds.generateOne
  private val dataset1 = datasets.generateOne.copy(
    maybeDescription = Some(datasetDescriptions.generateOne),
    published        = datasetPublishingInfos.generateOne.copy(maybeDate = Some(datasetPublishedDates.generateOne)),
    project          = List(DatasetProject(project.path, projectName))
  )
  private val dataset2CommitId = commitIds.generateOne
  private val dataset2 = datasets.generateOne.copy(
    maybeDescription = None,
    published        = datasetPublishingInfos.generateOne.copy(maybeDate = None),
    project          = List(DatasetProject(project.path, projectName))
  )

  feature("GraphQL query to find project's datasets") {

    scenario("As a user I would like to find project's datasets with a GraphQL query") {

      Given("some data in the RDF Store")
      val jsonLDTriples = triples(
        singleFileAndCommitWithDataset(
          project.path,
          projectName,
          commitId                  = dataset1CommitId,
          committerName             = dataset1.created.agent.name,
          committerEmail            = dataset1.created.agent.email,
          datasetIdentifier         = dataset1.id,
          datasetName               = dataset1.name,
          maybeDatasetDescription   = dataset1.maybeDescription,
          datasetCreatedDate        = dataset1.created.date,
          maybeDatasetPublishedDate = dataset1.published.maybeDate,
          maybeDatasetCreators      = dataset1.published.creators.map(creator => (creator.name, creator.maybeEmail)),
          maybeDatasetParts         = dataset1.part.map(part => (part.name, part.atLocation, part.dateCreated)),
          schemaVersion             = currentSchemaVersion
        ),
        singleFileAndCommitWithDataset(
          project.path,
          projectName,
          commitId                  = dataset2CommitId,
          committerName             = dataset2.created.agent.name,
          committerEmail            = dataset2.created.agent.email,
          datasetIdentifier         = dataset2.id,
          datasetName               = dataset2.name,
          maybeDatasetDescription   = dataset2.maybeDescription,
          datasetCreatedDate        = dataset2.created.date,
          maybeDatasetPublishedDate = dataset2.published.maybeDate,
          maybeDatasetCreators      = dataset2.published.creators.map(creator => (creator.name, creator.maybeEmail)),
          maybeDatasetParts         = dataset2.part.map(part => (part.name, part.atLocation, part.dateCreated)),
          schemaVersion             = currentSchemaVersion
        )
      )

      `data in the RDF store`(project, dataset1CommitId, jsonLDTriples)

      When("user posts a graphql query to fetch datasets")
      val response = knowledgeGraphClient POST query

      Then("he should get OK response with project's datasets in Json")
      response.status shouldBe Ok

      val Right(responseJson) = response.bodyAsJson.hcursor.downField("data").downField("datasets").as[List[Json]]

      val actual   = responseJson flatMap sortCreators
      val expected = List(json(dataset1), json(dataset2)) flatMap sortCreators flatMap sortPartsAlphabetically
      actual should contain theSameElementsAs expected
    }

    scenario("As a user I would like to find project's datasets with a named GraphQL query") {

      Given("some data in the RDF Store")

      When("user posts a graphql query to fetch the datasets")
      val response = knowledgeGraphClient.POST(
        namedQuery,
        variables = Map("projectPath" -> project.path.toString)
      )

      Then("he should get OK response with project's datasets in Json")
      response.status shouldBe Ok

      val Right(responseJson) = response.bodyAsJson.hcursor.downField("data").downField("datasets").as[List[Json]]

      val actual   = responseJson flatMap sortCreators
      val expected = List(json(dataset1), json(dataset2)) flatMap sortCreators flatMap sortPartsAlphabetically
      actual should contain theSameElementsAs expected
    }
  }

  private def sortCreators(json: Json) = {

    def orderByName(creators: Vector[Json]): Vector[Json] = creators.sortWith {
      case (json1, json2) =>
        (json1.hcursor.get[String]("name").toOption -> json2.hcursor.get[String]("name").toOption)
          .mapN(_ < _)
          .getOrElse(false)
    }

    json.hcursor.downField("published").downField("creator").withFocus(_.mapArray(orderByName)).top
  }

  private def sortPartsAlphabetically(json: Json) = {

    def orderByName(parts: Vector[Json]): Vector[Json] = parts.sortWith {
      case (json1, json2) =>
        (json1.hcursor.get[String]("name").toOption -> json2.hcursor.get[String]("name").toOption)
          .mapN(_ < _)
          .getOrElse(false)
    }

    json.hcursor.downField("hasPart").withFocus(_.mapArray(orderByName)).top
  }

  private val query: Document = graphql"""
    {
      datasets(projectPath: "namespace/datasets-project") {
        identifier
        name
        description
        created { dateCreated agent { email name } }
        published { datePublished creator { name email } }
        hasPart { name atLocation dateCreated }
        isPartOf { path name }
      }
    }"""

  private val namedQuery: Document = graphql"""
    query($$projectPath: ProjectPath!) { 
      datasets(projectPath: $$projectPath) { 
        identifier
        name
        description
        created { dateCreated agent { email name } }
        published { datePublished creator { name email } }
        hasPart { name atLocation dateCreated }
        isPartOf { path name }
      }
    }"""

  // format: off
  private def json(dataset: Dataset) = json"""
    {
      "identifier": ${dataset.id}, 
      "name": ${dataset.name},
      "description": ${dataset.maybeDescription.map(_.asJson).getOrElse(Json.Null)},
      "created": {
        "dateCreated": ${dataset.created.date},
        "agent": {
          "email": ${dataset.created.agent.email},
          "name": ${dataset.created.agent.name}
        }
      },
      "published": {
        "datePublished": ${dataset.published.maybeDate.map(_.asJson).getOrElse(Json.Null)},
        "creator": ${dataset.published.creators.toList}
      },
      "hasPart": ${dataset.part},
      "isPartOf": ${dataset.project}
    }"""
  // format: on

  private implicit lazy val creatorEncoder: Encoder[DatasetCreator] = Encoder.instance[DatasetCreator] { creator =>
    json"""{
        "email": ${creator.maybeEmail.map(_.asJson).getOrElse(Json.Null)},
        "name": ${creator.name}
      }"""
  }

  private implicit lazy val partEncoder: Encoder[DatasetPart] = Encoder.instance[DatasetPart] { part =>
    json"""{
        "name": ${part.name},
        "atLocation": ${part.atLocation},
        "dateCreated": ${part.dateCreated}
      }"""
  }

  private implicit lazy val projectEncoder: Encoder[DatasetProject] = Encoder.instance[DatasetProject] { project =>
    json"""{
        "path": ${project.path},
        "name": ${project.name}
      }"""
  }
}
