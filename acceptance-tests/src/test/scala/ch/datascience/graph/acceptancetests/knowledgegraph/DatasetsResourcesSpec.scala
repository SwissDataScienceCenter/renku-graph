/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

import cats.data.NonEmptyList
import cats.syntax.all._
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
import ch.datascience.graph.model.datasets.{Description, Identifier, Name, PublishedDate, Title}
import ch.datascience.graph.model.events.{CommitId, CommittedDate}
import ch.datascience.graph.model.users.{Name => UserName}
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.UrlEncoder.urlEncode
import ch.datascience.http.rest.Links.{Href, Rel, _links}
import ch.datascience.http.server.EndpointTester._
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.knowledgegraph.projects.ProjectsGenerators._
import ch.datascience.knowledgegraph.projects.model.Project
import ch.datascience.rdfstore.entities.EntitiesGenerators.persons
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

    val user = authUsers.generateOne
    implicit val accessToken: AccessToken = user.accessToken

    val project = {
      val initProject = projects.generateOne
      initProject.copy(
        maybeDescription = projectDescriptions.generateSome,
        forking = initProject.forking.copy(maybeParent = None),
        created = initProject.created.copy(maybeCreator = None)
      )
    }
    val dataset1Creation = addedToProjectObjects.generateOne.copy(
      agent = DatasetAgent(project.created.maybeCreator.flatMap(_.maybeEmail),
                           project.created.maybeCreator.map(_.name).getOrElse(userNames.generateOne)
      )
    )
    val dataset1CommitId  = commitIds.generateOne
    val dataset1Committer = Person(dataset1Creation.agent.name, dataset1Creation.agent.maybeEmail)
    val dataset1 = nonModifiedDatasets().generateOne.copy(
      maybeDescription = Some(datasetDescriptions.generateOne),
      usedIn = List(DatasetProject(project.path, project.name, dataset1Creation))
    )
    val dataset2Creation = addedToProjectObjects.generateOne
    val dataset2CommitId = commitIds.generateOne
    val dataset2 = nonModifiedDatasets().generateOne.copy(
      maybeDescription = None,
      usedIn = List(DatasetProject(project.path, project.name, dataset2Creation))
    )
    val modifiedDataset2 = modifiedDatasetsOnFirstProject(dataset2).generateOne

    Scenario("As a user I would like to find project's datasets by calling a REST endpoint") {

      Given("some data in the RDF Store")
      val jsonLDTriples = JsonLD.arr(
        nonModifiedDataSetCommit(
          commitId = dataset1CommitId,
          committedDate = dataset1Creation.date.toUnsafe(date => CommittedDate.from(date.value)),
          committer = dataset1Committer,
          cliVersion = currentVersionPair.cliVersion
        )(
          projectPath = project.path,
          projectName = project.name,
          projectDateCreated = project.created.date,
          maybeProjectCreator = project.created.maybeCreator.map(creator => Person(creator.name, creator.maybeEmail)),
          projectVersion = project.version
        )(
          datasetIdentifier = dataset1.id,
          datasetTitle = dataset1.title,
          datasetName = dataset1.name,
          maybeDatasetSameAs = dataset1.sameAs.some,
          maybeDatasetDescription = dataset1.maybeDescription,
          dates = dataset1.dates,
          datasetCreators = dataset1.creators map toPerson,
          datasetParts = dataset1.parts.map(part => (part.name, part.atLocation)),
          datasetImages = dataset1.images
        ),
        nonModifiedDataSetCommit(
          commitId = dataset2CommitId,
          committedDate = dataset2Creation.date.toUnsafe(date => CommittedDate.from(date.value)),
          committer = Person(dataset2Creation.agent.name, dataset2Creation.agent.maybeEmail),
          cliVersion = currentVersionPair.cliVersion
        )(
          projectPath = project.path,
          projectName = project.name,
          projectDateCreated = project.created.date,
          maybeProjectCreator = project.created.maybeCreator.map(creator => Person(creator.name, creator.maybeEmail)),
          projectVersion = project.version
        )(
          datasetIdentifier = dataset2.id,
          datasetTitle = dataset2.title,
          datasetName = dataset2.name,
          maybeDatasetSameAs = dataset2.sameAs.some,
          maybeDatasetDescription = dataset2.maybeDescription,
          dates = dataset2.dates,
          datasetCreators = dataset2.creators map toPerson,
          datasetParts = dataset2.parts.map(part => (part.name, part.atLocation)),
          datasetImages = dataset2.images
        ),
        modifiedDataSetCommit(
          committedDate = modifiedDataset2.usedIn.head.created.date.toUnsafe(date => CommittedDate.from(date.value)),
          committer = Person(modifiedDataset2.usedIn.head.created.agent.name,
                             modifiedDataset2.usedIn.head.created.agent.maybeEmail
          ),
          cliVersion = currentVersionPair.cliVersion
        )(
          projectPath = project.path,
          projectName = project.name,
          projectDateCreated = project.created.date,
          maybeProjectCreator = project.created.maybeCreator.map(creator => Person(creator.name, creator.maybeEmail)),
          projectVersion = project.version
        )(
          datasetIdentifier = modifiedDataset2.id,
          datasetTitle = modifiedDataset2.title,
          datasetName = modifiedDataset2.name,
          datasetDerivedFrom = modifiedDataset2.derivedFrom,
          maybeDatasetDescription = modifiedDataset2.maybeDescription,
          dates = modifiedDataset2.dates,
          datasetCreators = modifiedDataset2.creators map toPerson,
          datasetParts = modifiedDataset2.parts.map(part => (part.name, part.atLocation)),
          datasetImages = modifiedDataset2.images
        )
      )

      `data in the RDF store`(project, dataset1CommitId, dataset1Committer, jsonLDTriples)(
        NonEmptyList.of(dataset1Committer, persons.generateOne.copy(maybeGitLabId = user.id.some)).map(_.asMember())
      )

      `wait for events to be processed`(project.id)

      And("the project exists in GitLab")
      `GET <gitlabApi>/projects/:path returning OK with`(project, withStatistics = true)

      When("user fetches project's datasets with GET knowledge-graph/projects/<project-name>/datasets")
      val projectDatasetsResponse = knowledgeGraphClient GET s"knowledge-graph/projects/${project.path}/datasets"

      Then("he should get OK response with project's datasets")
      projectDatasetsResponse.status shouldBe Ok
      val Right(foundDatasets) = projectDatasetsResponse.bodyAsJson.as[List[Json]]
      foundDatasets should contain theSameElementsAs List(briefJson(dataset1), briefJson(modifiedDataset2))

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
      val expectedDataset = List(dataset1, modifiedDataset2)
        .find(dataset => someDatasetDetailsLink.value contains urlEncode(dataset.id.value))
        .getOrFail(message = "Returned 'details' link does not point to any dataset in the RDF store")
      findIdentifier(foundDatasetDetails) shouldBe expectedDataset.id

      When("user is authenticated")
      `GET <gitlabApi>/user returning OK`(user)

      And("he fetches details of the dataset project using the link from the response")
      val datasetProjectLink = foundDatasetDetails.hcursor
        .downField("isPartOf")
        .downArray
        ._links
        .get(Rel("project-details"))
        .getOrFail("No link with rel 'project-details'")

      val datasetUsedInProjectLink = foundDatasetDetails.hcursor
        .downField("usedIn")
        .downArray
        ._links
        .get(Rel("project-details"))
        .getOrFail("No link with rel 'project-details'")

      datasetProjectLink shouldBe datasetUsedInProjectLink

      foundDatasetDetails.hcursor
        .downField("project")
        ._links
        .get(Rel("project-details"))
        .getOrFail("No link with rel 'project-details'") shouldBe datasetUsedInProjectLink

      val getProjectResponse = restClient.GET(datasetUsedInProjectLink.toString, user.accessToken)

      Then("he should get OK response with project details")
      getProjectResponse.status     shouldBe Ok
      getProjectResponse.bodyAsJson shouldBe ProjectsResources.fullJson(project)

      When("user fetches initial version of the modified dataset with the link from the response")
      val modifiedDataset2Json = foundDatasets
        .find(json => findIdentifier(json) == modifiedDataset2.id)
        .getOrFail(s"No modified dataset with ${modifiedDataset2.id} id")

      val modifiedDatasetInitialVersionLink = modifiedDataset2Json.hcursor._links
        .get(Rel("initial-version"))
        .getOrFail("No link with rel 'initial-version'")
      val getInitialVersionResponse = restClient GET modifiedDatasetInitialVersionLink.toString

      Then("he should get OK response with project details")
      getInitialVersionResponse.status                     shouldBe Ok
      findIdentifier(getInitialVersionResponse.bodyAsJson) shouldBe dataset2.id
    }
  }

  Feature("GET knowledge-graph/datasets?query=<text> to find datasets with a free-text search") {

    Scenario("As a user I would like to be able to search for datasets by free-text search") {

      implicit val accessToken: AccessToken = accessTokens.generateOne

      val text             = nonBlankStrings(minLength = 10).generateOne
      val dataset1Projects = nonEmptyList(projects).generateOne.toList
      val dataset1 = nonModifiedDatasets().generateOne.copy(
        title = sentenceContaining(text).map(_.value).map(Title.apply).generateOne,
        usedIn = (dataset1Projects map toDatasetProject)
      )
      val dataset2Projects = nonEmptyList(projects).generateOne.toList
      val dataset2 = nonModifiedDatasets().generateOne.copy(
        maybeDescription = Some(sentenceContaining(text).map(_.value).map(Description.apply).generateOne),
        usedIn = (dataset2Projects map toDatasetProject)
      )
      val dataset3Projects = nonEmptyList(projects).generateOne.toList
      val dataset3 = {
        val dataset = nonModifiedDatasets().generateOne
        dataset.copy(
          creators = Set(
            datasetCreators.generateOne
              .copy(name = sentenceContaining(text).map(_.value).map(UserName.apply).generateOne)
          ),
          usedIn = (dataset3Projects map toDatasetProject)
        )
      }

      val dataset4Projects = nonEmptyList(projects).generateOne.toList
      val dataset4 = nonModifiedDatasets().generateOne.copy(
        name = sentenceContaining(text).map(_.value).map(Name.apply).generateOne,
        usedIn = (dataset4Projects map toDatasetProject)
      )

      val dataset5Projects = List(projects.generateOne)
      val dataset5 = nonModifiedDatasets().generateOne.copy(
        usedIn = (dataset5Projects map toDatasetProject)
      )

      Given("some datasets with title, description, name and author containing some arbitrary chosen text")
      val sameAs1Ids = pushToStore(dataset1, dataset1Projects)
      val sameAs2Ids = pushToStore(dataset2, dataset2Projects)
      val sameAs3Ids = pushToStore(dataset3, dataset3Projects)
      val sameAs4Ids = pushToStore(dataset4, dataset4Projects)
      val sameAs5Ids = pushToStore(dataset5, dataset5Projects)

      When("user calls the GET knowledge-graph/datasets?query=<text>")
      val datasetsSearchResponse = knowledgeGraphClient GET s"knowledge-graph/datasets?query=${urlEncode(text.value)}"

      Then("he should get OK response with some matching datasets")
      datasetsSearchResponse.status shouldBe Ok

      val Right(foundDatasets) = datasetsSearchResponse.bodyAsJson.as[List[Json]]
      foundDatasets.flatMap(sortCreators) should contain theSameElementsAs List(
        searchResultJson(dataset1, sameAs1Ids, foundDatasets),
        searchResultJson(dataset2, sameAs2Ids, foundDatasets),
        searchResultJson(dataset3, sameAs3Ids, foundDatasets),
        searchResultJson(dataset4, sameAs4Ids, foundDatasets)
      ).flatMap(sortCreators)

      When("user calls the GET knowledge-graph/datasets?query=<text>&sort=title:asc")
      val searchSortedByName =
        knowledgeGraphClient GET s"knowledge-graph/datasets?query=${urlEncode(text.value)}&sort=title:asc"

      Then("he should get OK response with some matching datasets sorted by title ASC")
      searchSortedByName.status shouldBe Ok

      val Right(foundDatasetsSortedByName) = searchSortedByName.bodyAsJson.as[List[Json]]
      val datasetsSortedByName = List(
        searchResultJson(dataset1, sameAs1Ids, foundDatasetsSortedByName),
        searchResultJson(dataset2, sameAs2Ids, foundDatasetsSortedByName),
        searchResultJson(dataset3, sameAs3Ids, foundDatasetsSortedByName),
        searchResultJson(dataset4, sameAs4Ids, foundDatasetsSortedByName)
      ).flatMap(sortCreators)
        .sortBy(_.hcursor.downField("title").as[String].getOrElse(fail("No 'title' property found")))
      foundDatasetsSortedByName.flatMap(sortCreators) shouldBe datasetsSortedByName

      When("user calls the GET knowledge-graph/datasets?query=<text>&sort=title:asc&page=2&per_page=1")
      val searchForPage =
        knowledgeGraphClient GET s"knowledge-graph/datasets?query=${urlEncode(text.value)}&sort=title:asc&page=2&per_page=1"

      Then("he should get OK response with the dataset from the requested page")
      val Right(foundDatasetsPage) = searchForPage.bodyAsJson.as[List[Json]]
      foundDatasetsPage.flatMap(sortCreators) should contain theSameElementsAs List(datasetsSortedByName(1))
        .flatMap(sortCreators)

      When("user calls the GET knowledge-graph/datasets?sort=name:asc")
      val searchWithoutPhrase = knowledgeGraphClient GET s"knowledge-graph/datasets?sort=title:asc"

      Then("he should get OK response with all the datasets")
      val Right(foundDatasetsWithoutPhrase) = searchWithoutPhrase.bodyAsJson.as[List[Json]]
      foundDatasetsWithoutPhrase.flatMap(sortCreators) should contain allElementsOf List(
        searchResultJson(dataset1, sameAs1Ids, foundDatasetsWithoutPhrase),
        searchResultJson(dataset2, sameAs2Ids, foundDatasetsWithoutPhrase),
        searchResultJson(dataset3, sameAs3Ids, foundDatasetsWithoutPhrase),
        searchResultJson(dataset4, sameAs4Ids, foundDatasetsWithoutPhrase),
        searchResultJson(dataset5, sameAs5Ids, foundDatasetsWithoutPhrase)
      ).flatMap(sortCreators)
        .sortBy(_.hcursor.downField("title").as[String].getOrElse(fail("No 'title' property found")))

      When("user uses the response header link with the rel='first'")
      val firstPageLink     = searchForPage.headerLink(rel = "first")
      val firstPageResponse = restClient GET firstPageLink

      Then("he should get OK response with the datasets from the first page")
      val Right(foundFirstPage) = firstPageResponse.bodyAsJson.as[List[Json]]
      foundFirstPage.flatMap(sortCreators) should contain theSameElementsAs List(datasetsSortedByName.head)
        .flatMap(sortCreators)

      When("user uses 'details' link of one of the found datasets")
      val someDatasetJson = Random.shuffle(foundDatasets).head

      val detailsLinkResponse = restClient GET someDatasetJson._links
        .get(Rel("details"))
        .getOrFail(message = "No link with rel 'details'")
        .toString
      detailsLinkResponse.status shouldBe Ok

      Then("he should get the same result as he'd call the knowledge-graph/datasets/:id endpoint directly")
      val datasetDetailsResponse =
        knowledgeGraphClient GET s"knowledge-graph/datasets/${urlEncode(findIdentifier(someDatasetJson).toString)}"
      detailsLinkResponse.bodyAsJson shouldBe datasetDetailsResponse.bodyAsJson
    }

    def pushToStore(dataset: NonModifiedDataset, projects: List[Project])(implicit
        accessToken:         AccessToken
    ): List[Identifier] = {
      val firstProject +: otherProjects = projects

      val commitId      = commitIds.generateOne
      val committer     = persons.generateOne
      val committedDate = committedDates.generateOne
      val datasetJsonLD = toDataSetCommit(firstProject, commitId, committer, committedDate, dataset)
      `data in the RDF store`(firstProject, commitId, committer, datasetJsonLD)()
      `wait for events to be processed`(firstProject.id)

      otherProjects.foldLeft(List(dataset.id)) { (datasetsIds, project) =>
        val commitId  = commitIds.generateOne
        val committer = persons.generateOne
        val datasetId = datasetIdentifiers.generateOne

        `data in the RDF store`(
          project,
          commitId,
          committer,
          toDataSetCommit(project,
                          commitId,
                          committer,
                          CommittedDate(committedDate.value plusSeconds positiveInts().generateOne.value),
                          dataset,
                          datasetId.some
          )
        )()

        `wait for events to be processed`(project.id)

        datasetsIds :+ datasetId
      }
    }

    def toDataSetCommit(project:              Project,
                        commitId:             CommitId,
                        committer:            Person,
                        committedDate:        CommittedDate,
                        dataset:              NonModifiedDataset,
                        overriddenIdentifier: Option[Identifier] = None
    ): JsonLD =
      nonModifiedDataSetCommit(
        commitId = commitId,
        committer = committer,
        committedDate = committedDate,
        cliVersion = currentVersionPair.cliVersion
      )(
        projectPath = project.path,
        projectName = project.name,
        projectVersion = project.version
      )(
        datasetIdentifier = overriddenIdentifier getOrElse dataset.id,
        datasetTitle = dataset.title,
        datasetName = dataset.name,
        maybeDatasetSameAs = dataset.sameAs.some,
        maybeDatasetDescription = dataset.maybeDescription,
        dates = dataset.dates,
        datasetCreators = dataset.creators map toPerson,
        datasetImages = dataset.images
      )

    def toDatasetProject(project: Project) =
      DatasetProject(project.path, project.name, addedToProjectObjects.generateOne)
  }
}

object DatasetsResources {

  import ch.datascience.json.JsonOps._
  import ch.datascience.tinytypes.json.TinyTypeEncoders._

  def briefJson(dataset: NonModifiedDataset): Json = json"""{
    "identifier": ${dataset.id.value},
    "versions": {
      "initial": ${dataset.versions.initial.value}
    },
    "title": ${dataset.title.value},
    "name": ${dataset.name.value},
    "sameAs": ${dataset.sameAs.value},
    "images": ${dataset.images.map(_.value)}
  }""" deepMerge {
    _links(
      Rel("details")         -> Href(renkuResourcesUrl / "datasets" / dataset.id),
      Rel("initial-version") -> Href(renkuResourcesUrl / "datasets" / dataset.versions.initial)
    )
  }

  def briefJson(dataset: ModifiedDataset): Json = json"""{
    "identifier": ${dataset.id.value},
    "versions": {
      "initial": ${dataset.versions.initial.value}
    },
    "title": ${dataset.title.value},
    "name": ${dataset.name.value},
    "derivedFrom": ${dataset.derivedFrom.value},
    "images": ${dataset.images.map(_.value)}
  }""" deepMerge {
    _links(
      Rel("details")         -> Href(renkuResourcesUrl / "datasets" / dataset.id),
      Rel("initial-version") -> Href(renkuResourcesUrl / "datasets" / dataset.versions.initial)
    )
  }

  def searchResultJson(dataset: Dataset, sameAsIds: List[Identifier], actualResults: List[Json]): Json = {
    val actualIdentifier = actualResults
      .findId(dataset.title)
      .getOrElse(fail(s"No ${dataset.title} dataset found among the results"))

    sameAsIds should contain(actualIdentifier)

    json"""{
      "identifier": ${actualIdentifier.value},
      "title": ${dataset.title.value},
      "name": ${dataset.name.value},
      "published": ${dataset.creators -> dataset.dates.maybeDatePublished},
      "date": ${dataset.dates.date}, 
      "projectsCount": ${dataset.usedIn.size},
      "images": ${dataset.images.map(_.value)}
    }"""
      .addIfDefined("description" -> dataset.maybeDescription)
      .deepMerge {
        _links(
          Rel("details") -> Href(renkuResourcesUrl / "datasets" / actualIdentifier)
        )
      }
  }

  private implicit lazy val publishingEncoder: Encoder[(Set[DatasetCreator], Option[PublishedDate])] =
    Encoder.instance[(Set[DatasetCreator], Option[PublishedDate])] {
      case (creators, Some(date)) =>
        json"""{
          "creator": ${creators.toList},
          "datePublished": $date
        }"""
      case (creators, None) =>
        json"""{
          "creator": ${creators.toList}
        }"""
    }

  private implicit lazy val creatorEncoder: Encoder[DatasetCreator] = Encoder.instance[DatasetCreator] {
    case DatasetCreator(maybeEmail, name, _) =>
      json"""{
        "name": $name
      }""" addIfDefined ("email" -> maybeEmail)
  }

  def sortCreators(json: Json): Option[Json] = {

    def orderByName(creators: Vector[Json]): Vector[Json] = creators.sortWith { case (json1, json2) =>
      (json1.hcursor.get[String]("name").toOption -> json2.hcursor.get[String]("name").toOption)
        .mapN(_ < _)
        .getOrElse(false)
    }

    json.hcursor.downField("published").downField("creator").withFocus(_.mapArray(orderByName)).top
  }

  lazy val toPerson: DatasetCreator => Person = creator => Person(creator.name, creator.maybeEmail, None)

  implicit class JsonsOps(jsons: List[Json]) {

    def findId(title: Title): Option[Identifier] =
      jsons
        .find(_.hcursor.downField("title").as[String].fold(throw _, _ == title.toString))
        .map(_.hcursor.downField("identifier").as[Identifier].fold(throw _, identity))
  }

  def findIdentifier(json: Json): Identifier =
    json.hcursor.downField("identifier").as[Identifier].fold(throw _, identity)
}
