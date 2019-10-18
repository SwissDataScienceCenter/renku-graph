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
import ch.datascience.generators.Generators.{nonBlankStrings, sentenceContaining}
import ch.datascience.graph.acceptancetests.data._
import ch.datascience.graph.acceptancetests.flows.RdfStoreProvisioning.`data in the RDF store`
import ch.datascience.graph.acceptancetests.testing.AcceptanceTestPatience
import ch.datascience.graph.acceptancetests.tooling.GraphServices
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.acceptancetests.tooling.TestReadabilityTools._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{Description, Identifier, Name}
import ch.datascience.graph.model.events.CommittedDate
import ch.datascience.graph.model.users.{Name => UserName}
import ch.datascience.http.client.UrlEncoder.urlEncode
import ch.datascience.http.rest.Links.{Href, Link, Rel, _links}
import ch.datascience.http.server.EndpointTester._
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.knowledgegraph.projects.ProjectsGenerators.{projects => projectsGen}
import ch.datascience.rdfstore.triples.{singleFileAndCommitWithDataset, triples}
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import eu.timepit.refined.auto._
import io.circe.Json
import io.circe.literal._
import org.http4s.Status._
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

import scala.util.Random

class DatasetsResourcesSpec extends FeatureSpec with GivenWhenThen with GraphServices with AcceptanceTestPatience {

  import DatasetsResources._

  feature("GET knowledge-graph/projects/<namespace>/<name>/datasets to find project's datasets") {

    val project          = projectsGen.generateOne
    val dataset1CommitId = commitIds.generateOne
    val dataset1Creation = datasetInProjectCreations.generateOne
    val dataset1 = datasets.generateOne.copy(
      maybeDescription = Some(datasetDescriptions.generateOne),
      published        = datasetPublishingInfos.generateOne.copy(maybeDate = Some(datasetPublishedDates.generateOne)),
      project          = List(DatasetProject(project.path, project.name, dataset1Creation))
    )
    val dataset2Creation = datasetInProjectCreations.generateOne
    val dataset2CommitId = commitIds.generateOne
    val dataset2 = datasets.generateOne.copy(
      maybeDescription = None,
      published        = datasetPublishingInfos.generateOne.copy(maybeDate = None),
      project          = List(DatasetProject(project.path, project.name, dataset2Creation))
    )

    scenario("As a user I would like to find project's datasets by calling a REST enpoint") {

      Given("some data in the RDF Store")
      val jsonLDTriples = triples(
        singleFileAndCommitWithDataset(
          projectPath               = project.path,
          projectName               = project.name,
          projectDateCreated        = project.created.date,
          projectCreator            = project.created.creator.name -> project.created.creator.email,
          commitId                  = dataset1CommitId,
          committerName             = dataset1Creation.agent.name,
          committerEmail            = dataset1Creation.agent.email,
          committedDate             = dataset1Creation.date.toUnsafe(date => CommittedDate.from(date.value)),
          datasetIdentifier         = dataset1.id,
          datasetName               = dataset1.name,
          maybeDatasetDescription   = dataset1.maybeDescription,
          maybeDatasetPublishedDate = dataset1.published.maybeDate,
          maybeDatasetCreators      = dataset1.published.creators.map(creator => (creator.name, creator.maybeEmail)),
          maybeDatasetParts         = dataset1.part.map(part => (part.name, part.atLocation)),
          schemaVersion             = currentSchemaVersion
        ),
        singleFileAndCommitWithDataset(
          projectPath               = project.path,
          projectName               = project.name,
          projectDateCreated        = project.created.date,
          projectCreator            = project.created.creator.name -> project.created.creator.email,
          commitId                  = dataset2CommitId,
          committerName             = dataset2Creation.agent.name,
          committerEmail            = dataset2Creation.agent.email,
          committedDate             = dataset2Creation.date.toUnsafe(date => CommittedDate.from(date.value)),
          datasetIdentifier         = dataset2.id,
          datasetName               = dataset2.name,
          maybeDatasetDescription   = dataset2.maybeDescription,
          maybeDatasetPublishedDate = dataset2.published.maybeDate,
          maybeDatasetCreators      = dataset2.published.creators.map(creator => (creator.name, creator.maybeEmail)),
          maybeDatasetParts         = dataset2.part.map(part => (part.name, part.atLocation)),
          schemaVersion             = currentSchemaVersion
        )
      )

      `data in the RDF store`(project.toGitLabProject(), dataset1CommitId, jsonLDTriples)

      When("user fetches project's datasets with GET knowledge-graph/projects/<project-name>/datasets")
      val projectDatasetsResponse = knowledgeGraphClient GET s"knowledge-graph/projects/${project.path}/datasets"

      Then("he should get OK response with project's datasets")
      projectDatasetsResponse.status shouldBe Ok
      val Right(foundDatasets) = projectDatasetsResponse.bodyAsJson.as[List[Json]]
      foundDatasets should contain theSameElementsAs List(briefJson(dataset1), briefJson(dataset2))

      When("user then fetches details of the chosen dataset with the link from the response")
      val someDatasetDetailsLink =
        Random
          .shuffle(foundDatasets)
          .headOption
          .flatMap(_._links.get(Rel("details")))
          .getOrFail(message = "No link with rel 'details'")
      val datasetDetailsResponse = restClient GET someDatasetDetailsLink.toString

      Then("he should get OK response with dataset details")
      datasetDetailsResponse.status shouldBe Ok
      val foundDatasetDetails = datasetDetailsResponse.bodyAsJson
      val expectedDataset = List(dataset1, dataset2)
        .find(dataset => someDatasetDetailsLink.value contains urlEncode(dataset.id.value))
        .getOrFail(message = "Returned 'details' link does not point to any dataset in the RDF store")
      foundDatasetDetails.hcursor.downField("identifier").as[Identifier] shouldBe Right(expectedDataset.id)

      When("user fetches details of the dataset project with the link from the response")
      val datasetProjectLink = foundDatasetDetails.hcursor
        .downField("isPartOf")
        .downArray
        ._links
        .get(Rel("project-details"))
        .getOrFail("No link with rel 'project-details'")
      val getProjectResponse = restClient GET datasetProjectLink.toString

      Then("he should get OK response with project details")
      getProjectResponse.status     shouldBe Ok
      getProjectResponse.bodyAsJson shouldBe ProjectsResources.fullJson(project)
    }
  }

  feature("GET knowledge-graph/datasets?query=<text> to find datasets with a free-text search") {

    scenario("As a user I would like to be able to search for datasets by some free-text search") {

      val text             = nonBlankStrings(minLength = 10).generateOne
      val dataset1Project  = projectsGen.generateOne
      val dataset1Commit   = commitIds.generateOne
      val dataset1Creation = datasetInProjectCreations.generateOne
      val dataset1 = datasets.generateOne.copy(
        name    = sentenceContaining(text).map(_.value).map(Name.apply).generateOne,
        project = List(DatasetProject(dataset1Project.path, dataset1Project.name, dataset1Creation))
      )
      val dataset2Project  = projectsGen.generateOne
      val dataset2Commit   = commitIds.generateOne
      val dataset2Creation = datasetInProjectCreations.generateOne
      val dataset2 = datasets.generateOne.copy(
        maybeDescription = Some(sentenceContaining(text).map(_.value).map(Description.apply).generateOne),
        project          = List(DatasetProject(dataset2Project.path, dataset2Project.name, dataset2Creation))
      )
      val dataset3Project  = projectsGen.generateOne
      val dataset3Commit   = commitIds.generateOne
      val dataset3Creation = datasetInProjectCreations.generateOne
      val dataset3 = {
        val dataset = datasets.generateOne
        dataset.copy(
          published = dataset.published.copy(
            creators = Set(
              datasetCreators.generateOne.copy(
                name = sentenceContaining(text).map(_.value).map(UserName.apply).generateOne))
          ),
          project = List(DatasetProject(dataset3Project.path, dataset3Project.name, dataset3Creation))
        )
      }
      val dataset4Project  = projectsGen.generateOne
      val dataset4Commit   = commitIds.generateOne
      val dataset4Creation = datasetInProjectCreations.generateOne
      val dataset4 = datasets.generateOne.copy(
        project = List(DatasetProject(dataset4Project.path, dataset4Project.name, dataset4Creation))
      )

      Given("some datasets with description, name and author containing some arbitrary chosen text")
      `data in the RDF store`(
        dataset1Project.toGitLabProject(),
        dataset1Commit,
        triples(
          singleFileAndCommitWithDataset(
            projectPath               = dataset1Project.path,
            commitId                  = dataset1Commit,
            datasetIdentifier         = dataset1.id,
            datasetName               = dataset1.name,
            maybeDatasetDescription   = dataset1.maybeDescription,
            maybeDatasetPublishedDate = dataset1.published.maybeDate,
            maybeDatasetCreators      = dataset1.published.creators.map(creator => (creator.name, creator.maybeEmail)),
            schemaVersion             = currentSchemaVersion
          )
        )
      )
      `data in the RDF store`(
        dataset2Project.toGitLabProject(),
        dataset2Commit,
        triples(
          singleFileAndCommitWithDataset(
            projectPath               = dataset2Project.path,
            commitId                  = dataset2Commit,
            datasetIdentifier         = dataset2.id,
            datasetName               = dataset2.name,
            maybeDatasetDescription   = dataset2.maybeDescription,
            maybeDatasetPublishedDate = dataset2.published.maybeDate,
            maybeDatasetCreators      = dataset2.published.creators.map(creator => (creator.name, creator.maybeEmail)),
            schemaVersion             = currentSchemaVersion
          )
        )
      )
      `data in the RDF store`(
        dataset3Project.toGitLabProject(),
        dataset3Commit,
        triples(
          singleFileAndCommitWithDataset(
            projectPath               = dataset3Project.path,
            commitId                  = dataset3Commit,
            datasetIdentifier         = dataset3.id,
            datasetName               = dataset3.name,
            maybeDatasetDescription   = dataset3.maybeDescription,
            maybeDatasetPublishedDate = dataset3.published.maybeDate,
            maybeDatasetCreators      = dataset3.published.creators.map(creator => (creator.name, creator.maybeEmail)),
            schemaVersion             = currentSchemaVersion
          )
        )
      )
      `data in the RDF store`(
        dataset4Project.toGitLabProject(),
        dataset4Commit,
        triples(
          singleFileAndCommitWithDataset(
            projectPath               = dataset4Project.path,
            commitId                  = dataset4Commit,
            datasetIdentifier         = dataset4.id,
            datasetName               = dataset4.name,
            maybeDatasetDescription   = dataset4.maybeDescription,
            maybeDatasetPublishedDate = dataset4.published.maybeDate,
            maybeDatasetCreators      = dataset4.published.creators.map(creator => (creator.name, creator.maybeEmail)),
            schemaVersion             = currentSchemaVersion
          )
        )
      )

      When("user calls the GET knowledge-graph/datasets?query=<text>")
      val datasetsSearchResponse = knowledgeGraphClient GET s"knowledge-graph/datasets?query=${urlEncode(text.value)}"

      Then("he should get OK response with matching datasets")
      datasetsSearchResponse.status shouldBe Ok

      val Right(foundDatasets) = datasetsSearchResponse.bodyAsJson.as[List[Json]]
      foundDatasets should contain theSameElementsAs List(
        searchResultJson(dataset1),
        searchResultJson(dataset2),
        searchResultJson(dataset3)
      )
    }
  }
}

object DatasetsResources {

  def briefJson(dataset: Dataset): Json = json"""
    {
      "identifier": ${dataset.id.value}, 
      "name": ${dataset.name.value}
    }""" deepMerge {
    _links(
      Link(Rel("details"), Href(renkuResourceUrl / "datasets" / dataset.id))
    )
  }

  def searchResultJson(dataset: Dataset): Json =
    json"""
    {
      "identifier": ${dataset.id.value}, 
      "name": ${dataset.name.value}
    }"""
      .deepMerge {
        dataset.maybeDescription.fold(Json.obj()) { description =>
          json"""{
            "description": ${description.value}
          }"""
        }
      }
      .deepMerge {
        _links(
          Link(Rel("details"), Href(renkuResourceUrl / "datasets" / dataset.id))
        )
      }
}
