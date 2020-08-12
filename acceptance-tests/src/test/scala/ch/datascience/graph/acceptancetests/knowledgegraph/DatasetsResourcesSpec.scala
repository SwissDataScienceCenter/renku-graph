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

import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.acceptancetests.data._
import ch.datascience.graph.acceptancetests.flows.RdfStoreProvisioning._
import ch.datascience.graph.acceptancetests.stubs.GitLab._
import ch.datascience.graph.acceptancetests.testing.AcceptanceTestPatience
import ch.datascience.graph.acceptancetests.tooling.GraphServices
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.acceptancetests.tooling.TestReadabilityTools._
import ch.datascience.graph.model.EventsGenerators.{commitIds, committedDates}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{Description, Identifier, Title}
import ch.datascience.graph.model.events.{CommitId, CommittedDate}
import ch.datascience.graph.model.users.{Name => UserName}
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.UrlEncoder.urlEncode
import ch.datascience.http.rest.Links.{Href, Link, Rel, _links}
import ch.datascience.http.server.EndpointTester._
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.knowledgegraph.projects.ProjectsGenerators._
import ch.datascience.knowledgegraph.projects.model.Project
import ch.datascience.rdfstore.entities.Person
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import eu.timepit.refined.auto._
import io.circe.literal._
import io.circe.{Encoder, Json}
import io.renku.jsonld.JsonLD
import org.http4s.Status._
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should

import scala.util.Random

class DatasetsResourcesSpec
    extends AnyFeatureSpec
    with GivenWhenThen
    with GraphServices
    with AcceptanceTestPatience
    with should.Matchers {

  import DatasetsResources._

  Feature("GET knowledge-graph/projects/<namespace>/<name>/datasets to find project's datasets") {

    implicit val accessToken: AccessToken = accessTokens.generateOne

    val project = {
      val initProject = projects.generateOne
      initProject.copy(
        maybeDescription = projectDescriptions.generateSome,
        forking          = initProject.forking.copy(maybeParent = None)
      )
    }
    val dataset1CommitId = commitIds.generateOne
    val dataset1Creation = addedToProject.generateOne.copy(
      agent = DatasetAgent(project.created.maybeCreator.flatMap(_.maybeEmail),
                           project.created.maybeCreator.map(_.name).getOrElse(userNames.generateOne))
    )
    val dataset1 = datasets.generateOne.copy(
      maybeDescription = Some(datasetDescriptions.generateOne),
      published        = datasetPublishingInfos.generateOne.copy(maybeDate = Some(datasetPublishedDates.generateOne)),
      projects         = List(DatasetProject(project.path, project.name, dataset1Creation))
    )
    val dataset2Creation = addedToProject.generateOne
    val dataset2CommitId = commitIds.generateOne
    val dataset2 = datasets.generateOne.copy(
      maybeDescription = None,
      published        = datasetPublishingInfos.generateOne.copy(maybeDate = None),
      projects         = List(DatasetProject(project.path, project.name, dataset2Creation))
    )

    Scenario("As a user I would like to find project's data-sets by calling a REST endpoint") {

      Given("some data in the RDF Store")
      val jsonLDTriples = JsonLD.arr(
        dataSetCommit(
          commitId      = dataset1CommitId,
          committedDate = dataset1Creation.date.toUnsafe(date => CommittedDate.from(date.value)),
          committer     = Person(dataset1Creation.agent.name, dataset1Creation.agent.maybeEmail),
          cliVersion    = currentCliVersion
        )(
          projectPath         = project.path,
          projectName         = project.name,
          projectDateCreated  = project.created.date,
          maybeProjectCreator = project.created.maybeCreator.map(creator => Person(creator.name, creator.maybeEmail)),
          projectVersion      = project.version
        )(
          datasetIdentifier         = dataset1.id,
          datasetTitle              = dataset1.title,
          datasetName               = dataset1.name,
          maybeDatasetSameAs        = dataset1.sameAs.some,
          maybeDatasetDescription   = dataset1.maybeDescription,
          maybeDatasetPublishedDate = dataset1.published.maybeDate,
          datasetCreators           = dataset1.published.creators.map(toPerson),
          datasetParts              = dataset1.parts.map(part => (part.name, part.atLocation))
        ),
        dataSetCommit(
          commitId      = dataset2CommitId,
          committedDate = dataset2Creation.date.toUnsafe(date => CommittedDate.from(date.value)),
          committer     = Person(dataset2Creation.agent.name, dataset2Creation.agent.maybeEmail),
          cliVersion    = currentCliVersion
        )(
          projectPath         = project.path,
          projectName         = project.name,
          projectDateCreated  = project.created.date,
          maybeProjectCreator = project.created.maybeCreator.map(creator => Person(creator.name, creator.maybeEmail)),
          projectVersion      = project.version
        )(
          datasetIdentifier         = dataset2.id,
          datasetTitle              = dataset2.title,
          datasetName               = dataset2.name,
          maybeDatasetSameAs        = dataset2.sameAs.some,
          maybeDatasetDescription   = dataset2.maybeDescription,
          maybeDatasetPublishedDate = dataset2.published.maybeDate,
          datasetCreators           = dataset2.published.creators.map(toPerson),
          datasetParts              = dataset2.parts.map(part => (part.name, part.atLocation))
        )
      )

      `data in the RDF store`(project, dataset1CommitId, jsonLDTriples)

      `triples updates run`(
        (List(dataset1, dataset2)
          .flatMap(_.published.creators.map(_.maybeEmail))
          .toSet + dataset1Creation.agent.maybeEmail + dataset2Creation.agent.maybeEmail + project.created.maybeCreator
          .flatMap(_.maybeEmail)).flatten
      )

      And("the project exists in GitLab")
      `GET <gitlab>/api/v4/projects/:path returning OK with`(project, withStatistics = true)

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

  Feature("GET knowledge-graph/datasets?query=<text> to find datasets with a free-text search") {

    Scenario("As a user I would like to be able to search for datasets by some free-text search") {

      implicit val accessToken: AccessToken = accessTokens.generateOne

      val text             = nonBlankStrings(minLength = 10).generateOne
      val dataset1Projects = nonEmptyList(projects).generateOne.toList
      val dataset1 = datasets.generateOne.copy(
        title    = sentenceContaining(text).map(_.value).map(Title.apply).generateOne,
        projects = dataset1Projects map toDatasetProject
      )
      val dataset2Projects = nonEmptyList(projects).generateOne.toList
      val dataset2 = datasets.generateOne.copy(
        maybeDescription = Some(sentenceContaining(text).map(_.value).map(Description.apply).generateOne),
        projects         = dataset2Projects map toDatasetProject
      )
      val dataset3Projects = nonEmptyList(projects).generateOne.toList
      val dataset3 = {
        val dataset = datasets.generateOne
        dataset.copy(
          published = dataset.published.copy(
            creators = Set(
              datasetCreators.generateOne
                .copy(name = sentenceContaining(text).map(_.value).map(UserName.apply).generateOne)
            )
          ),
          projects = dataset3Projects map toDatasetProject
        )
      }
      val dataset4Projects = List(projects.generateOne)
      val dataset4 = datasets.generateOne.copy(
        projects = dataset4Projects map toDatasetProject
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

      When("user calls the GET knowledge-graph/datasets?query=<text>&sort=title:asc")
      val searchSortedByName = knowledgeGraphClient GET s"knowledge-graph/datasets?query=${urlEncode(text.value)}&sort=title:asc"

      Then("he should get OK response with some matching datasets sorted by title ASC")
      searchSortedByName.status shouldBe Ok

      val Right(foundDatasetsSortedByName) = searchSortedByName.bodyAsJson.as[List[Json]]
      val datasetsSortedByName = List(
        searchResultJson(dataset1),
        searchResultJson(dataset2),
        searchResultJson(dataset3)
      ).flatMap(sortCreators)
        .sortBy(_.hcursor.downField("title").as[String].getOrElse(fail("No 'name' property found")))
      foundDatasetsSortedByName.flatMap(sortCreators) shouldBe datasetsSortedByName

      When("user calls the GET knowledge-graph/datasets?query=<text>&sort=title:asc&page=2&per_page=1")
      val searchForPage = knowledgeGraphClient GET s"knowledge-graph/datasets?query=${urlEncode(text.value)}&sort=title:asc&page=2&per_page=1"

      Then("he should get OK response with the dataset from the requested page")
      val Right(foundDatasetsPage) = searchForPage.bodyAsJson.as[List[Json]]
      foundDatasetsPage.flatMap(sortCreators) should contain theSameElementsAs List(datasetsSortedByName(1))
        .flatMap(sortCreators)

      When("user calls the GET knowledge-graph/datasets?sort=name:asc")
      val searchWithoutPhrase = knowledgeGraphClient GET s"knowledge-graph/datasets?sort=title:asc"

      Then("he should get OK response with all the datasets")
      val Right(foundDatasetsWithoutPhrase) = searchWithoutPhrase.bodyAsJson.as[List[Json]]
      foundDatasetsWithoutPhrase.flatMap(sortCreators) should contain allElementsOf List(
        searchResultJson(dataset1),
        searchResultJson(dataset2),
        searchResultJson(dataset3),
        searchResultJson(dataset4)
      ).flatMap(sortCreators)
        .sortBy(_.hcursor.downField("title").as[String].getOrElse(fail("No 'name' property found")))

      When("user uses the response header link with the rel='first'")
      val firstPageLink     = searchForPage.headerLink(rel = "first")
      val firstPageResponse = restClient GET firstPageLink

      Then("he should get OK response with the datasets from the first page")
      val Right(foundFirstPage) = firstPageResponse.bodyAsJson.as[List[Json]]
      foundFirstPage.flatMap(sortCreators) should contain theSameElementsAs List(datasetsSortedByName.head)
        .flatMap(sortCreators)
    }

    def pushToStore(dataset: Dataset, projects: List[Project])(implicit accessToken: AccessToken): Unit = {
      val firstProject +: otherProjects = projects

      val commitId      = commitIds.generateOne
      val committedDate = committedDates.generateOne
      val datasetJsonLD = toDataSetCommit(firstProject, commitId, committedDate, dataset)
      `data in the RDF store`(firstProject, commitId, datasetJsonLD)
      `triples updates run`(dataset.published.creators.flatMap(_.maybeEmail))

      otherProjects foreach { project =>
        val commitId = commitIds.generateOne
        `data in the RDF store`(
          project,
          commitId,
          toDataSetCommit(project,
                          commitId,
                          CommittedDate(committedDate.value plusSeconds positiveInts().generateOne.value),
                          dataset,
                          datasetIdentifiers.generateSome)
        )
        `triples updates run`(dataset.published.creators.flatMap(_.maybeEmail))
      }
    }

    def toDataSetCommit(project:              Project,
                        commitId:             CommitId,
                        committedDate:        CommittedDate,
                        dataset:              Dataset,
                        overriddenIdentifier: Option[Identifier] = None) =
      dataSetCommit(
        commitId      = commitId,
        committedDate = committedDate,
        cliVersion    = currentCliVersion
      )(
        projectPath = project.path,
        projectName = project.name
      )(
        datasetIdentifier         = overriddenIdentifier getOrElse dataset.id,
        datasetTitle              = dataset.title,
        datasetName               = dataset.name,
        maybeDatasetSameAs        = dataset.sameAs.some,
        maybeDatasetDescription   = dataset.maybeDescription,
        maybeDatasetPublishedDate = dataset.published.maybeDate,
        datasetCreators           = dataset.published.creators map toPerson
      )

    def toDatasetProject(project: Project) =
      DatasetProject(project.path, project.name, addedToProject.generateOne)
  }
}

object DatasetsResources {

  import ch.datascience.json.JsonOps._
  import ch.datascience.tinytypes.json.TinyTypeEncoders._

  def briefJson(dataset: Dataset): Json = json"""{
    "identifier": ${dataset.id.value},
    "title": ${dataset.title.value},
    "name": ${dataset.name.value},
    "sameAs": ${dataset.sameAs.value}
  }""" deepMerge {
    _links(
      Link(Rel("details"), Href(renkuResourcesUrl / "datasets" / dataset.id))
    )
  }

  def searchResultJson(dataset: Dataset): Json =
    json"""{
      "identifier": ${dataset.id.value},
      "title": ${dataset.title.value},
      "name": ${dataset.name.value},
      "published": ${dataset.published},
      "projectsCount": ${dataset.projects.size}
    }"""
      .addIfDefined("description" -> dataset.maybeDescription)
      .deepMerge {
        _links(
          Link(Rel("details"), Href(renkuResourcesUrl / "datasets" / dataset.id))
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

  lazy val toPerson: DatasetCreator => Person = creator => Person(creator.name, creator.maybeEmail, None)
}
