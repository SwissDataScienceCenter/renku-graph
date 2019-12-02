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
import ch.datascience.generators.Generators._
import ch.datascience.graph.acceptancetests.data._
import ch.datascience.graph.acceptancetests.flows.RdfStoreProvisioning._
import ch.datascience.graph.acceptancetests.testing.AcceptanceTestPatience
import ch.datascience.graph.acceptancetests.tooling.GraphServices
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.acceptancetests.tooling.TestReadabilityTools._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{Description, Identifier, Name}
import ch.datascience.graph.model.events.{CommitId, CommittedDate}
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.graph.model.users.{Name => UserName}
import ch.datascience.http.client.UrlEncoder.urlEncode
import ch.datascience.http.rest.Links.{Href, Link, Rel, _links}
import ch.datascience.http.server.EndpointTester._
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.knowledgegraph.projects.ProjectsGenerators.{projects => projectsGen}
import ch.datascience.knowledgegraph.projects.model.Project
import ch.datascience.rdfstore.triples.{singleFileAndCommitWithDataset, triples}
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import eu.timepit.refined.auto._
import io.circe.literal._
import io.circe.{Encoder, Json}
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
          maybeDatasetCreators      = dataset1.published.creators.map(creator => (creator.name, creator.maybeEmail, None)),
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
          maybeDatasetCreators      = dataset2.published.creators.map(creator => (creator.name, creator.maybeEmail, None)),
          maybeDatasetParts         = dataset2.part.map(part => (part.name, part.atLocation)),
          schemaVersion             = currentSchemaVersion
        )
      )

      `data in the RDF store`(project.toGitLabProject(), dataset1CommitId, jsonLDTriples)

      `triples updates run`(
        List(dataset1, dataset2)
          .flatMap(_.published.creators.flatMap(_.maybeEmail))
          .toSet + dataset1Creation.agent.email + dataset2Creation.agent.email + project.created.creator.email
      )

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
      val dataset1Projects = nonEmptyList(projectsGen).generateOne.toList
      val dataset1 = datasets.generateOne.copy(
        name    = sentenceContaining(text).map(_.value).map(Name.apply).generateOne,
        project = dataset1Projects map toDatasetProject
      )
      val dataset2Projects = nonEmptyList(projectsGen).generateOne.toList
      val dataset2 = datasets.generateOne.copy(
        maybeDescription = Some(sentenceContaining(text).map(_.value).map(Description.apply).generateOne),
        project          = dataset2Projects map toDatasetProject
      )
      val dataset3Projects = nonEmptyList(projectsGen).generateOne.toList
      val dataset3 = {
        val dataset = datasets.generateOne
        dataset.copy(
          published = dataset.published.copy(
            creators = Set(
              datasetCreators.generateOne
                .copy(name = sentenceContaining(text).map(_.value).map(UserName.apply).generateOne)
            )
          ),
          project = dataset3Projects map toDatasetProject
        )
      }
      val dataset4Projects = List(projectsGen.generateOne)
      val dataset4 = datasets.generateOne.copy(
        project = dataset4Projects map toDatasetProject
      )

      Given("some datasets with description, name and author containing some arbitrary chosen text")
      pushToStore(dataset1, dataset1Projects)
      pushToStore(dataset2, dataset2Projects)
      pushToStore(dataset3, dataset3Projects)
      pushToStore(dataset4, dataset4Projects)

      When("user calls the GET knowledge-graph/datasets?query=<text>")
      val datasetsSearchResponse = knowledgeGraphClient GET s"knowledge-graph/datasets?query=${urlEncode(text.value)}"

      Then("he should get OK response with some matching datasets")
      datasetsSearchResponse.status shouldBe Ok

      val Right(foundDatasets) = datasetsSearchResponse.bodyAsJson.as[List[Json]]
      foundDatasets.flatMap(sortCreators) should contain theSameElementsAs List(
        searchResultJson(dataset1),
        searchResultJson(dataset2),
        searchResultJson(dataset3)
      ).flatMap(sortCreators)

      When("user calls the GET knowledge-graph/datasets?query=<text>&sort=name:asc")
      val searchSortedByName = knowledgeGraphClient GET s"knowledge-graph/datasets?query=${urlEncode(text.value)}&sort=name:asc"

      Then("he should get OK response with some matching datasets sorted by name ASC")
      searchSortedByName.status shouldBe Ok

      val Right(foundDatasetsSortedByName) = searchSortedByName.bodyAsJson.as[List[Json]]
      val datasetsSortedByName = List(
        searchResultJson(dataset1),
        searchResultJson(dataset2),
        searchResultJson(dataset3)
      ).flatMap(sortCreators).sortBy(_.hcursor.downField("name").as[String].getOrElse(fail("No 'name' property found")))
      foundDatasetsSortedByName.flatMap(sortCreators) shouldBe datasetsSortedByName

      When("user calls the GET knowledge-graph/datasets?query=<text>&sort=name:asc&page=2&per_page=1")
      val searchForPage = knowledgeGraphClient GET s"knowledge-graph/datasets?query=${urlEncode(text.value)}&sort=name:asc&page=2&per_page=1"

      Then("he should get OK response with the dataset from the requested page")
      val Right(foundDatasetsPage) = searchForPage.bodyAsJson.as[List[Json]]
      foundDatasetsPage should contain only datasetsSortedByName(1)
    }

    def pushToStore(dataset: Dataset, projects: List[Project]): Unit =
      projects foreach { project =>
        val commitId = commitIds.generateOne
        `data in the RDF store`(project.toGitLabProject(),
                                commitId,
                                triples(toSingleFileAndCommitWithDataset(project.path, commitId, dataset)))
        `triples updates run`(dataset.published.creators.flatMap(_.maybeEmail))
      }

    def toSingleFileAndCommitWithDataset(projectPath: ProjectPath, commitId: CommitId, dataset: Dataset): List[Json] =
      singleFileAndCommitWithDataset(
        projectPath               = projectPath,
        commitId                  = commitId,
        datasetIdentifier         = dataset.id,
        datasetName               = dataset.name,
        maybeDatasetDescription   = dataset.maybeDescription,
        maybeDatasetPublishedDate = dataset.published.maybeDate,
        maybeDatasetCreators      = dataset.published.creators.map(creator => (creator.name, creator.maybeEmail, None)),
        schemaVersion             = currentSchemaVersion
      )

    def toDatasetProject(project: Project) =
      DatasetProject(project.path, project.name, datasetInProjectCreations.generateOne)
  }
}

object DatasetsResources {

  import ch.datascience.json.JsonOps._
  import ch.datascience.tinytypes.json.TinyTypeEncoders._

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
    json"""{
      "identifier": ${dataset.id.value}, 
      "name": ${dataset.name.value},
      "published": ${dataset.published},
      "projectsCount": ${dataset.project.size}
    }"""
      .addIfDefined("description" -> dataset.maybeDescription)
      .deepMerge {
        _links(
          Link(Rel("details"), Href(renkuResourceUrl / "datasets" / dataset.id))
        )
      }

  private implicit lazy val publishingEncoder: Encoder[DatasetPublishing] = Encoder.instance[DatasetPublishing] {
    case DatasetPublishing(maybeDate, creators) =>
      json"""{
        "creator": ${creators.toList}
      }""" addIfDefined "datePublished" -> maybeDate
  }

  private implicit lazy val creatorEncoder: Encoder[DatasetCreator] = Encoder.instance[DatasetCreator] {
    case DatasetCreator(maybeEmail, name, _) =>
      json"""{
        "name": $name
      }""" addIfDefined ("email" -> maybeEmail)
  }

  def sortCreators(json: Json): Option[Json] = {
    import cats.implicits._

    def orderByName(creators: Vector[Json]): Vector[Json] = creators.sortWith {
      case (json1, json2) =>
        (json1.hcursor.get[String]("name").toOption -> json2.hcursor.get[String]("name").toOption)
          .mapN(_ < _)
          .getOrElse(false)
    }

    json.hcursor.downField("published").downField("creator").withFocus(_.mapArray(orderByName)).top
  }
}
