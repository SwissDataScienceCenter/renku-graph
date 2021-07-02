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

import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators.{accessTokens, authUsers}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.acceptancetests.data._
import ch.datascience.graph.acceptancetests.flows.RdfStoreProvisioning._
import ch.datascience.graph.acceptancetests.stubs.GitLab._
import ch.datascience.graph.acceptancetests.testing.AcceptanceTestPatience
import ch.datascience.graph.acceptancetests.tooling.GraphServices
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.acceptancetests.tooling.TestReadabilityTools._
import ch.datascience.graph.model
import ch.datascience.graph.model.datasets.{DatePublished, Identifier, Title}
import ch.datascience.graph.model.projects.Visibility
import ch.datascience.graph.model.testentities.ModelOps.DatasetForkingResult
import ch.datascience.graph.model.testentities.{gitLabApiUrl => _, renkuBaseUrl => _, _}
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.UrlEncoder.urlEncode
import ch.datascience.http.rest.Links.{Href, Rel, _links}
import ch.datascience.http.server.EndpointTester._
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import eu.timepit.refined.auto._
import io.circe.literal._
import io.circe.{Encoder, Json}
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import org.http4s.Status._
import org.scalacheck.Gen
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

    val project = dataProjects(
      projectEntities[model.projects.ForksCount.Zero](visibilityPublic).generateOne
    ).generateOne
    val dataset1         = datasetEntities(datasetProvenanceInternal, fixed(project.entitiesProject)).generateOne
    val dataset2         = datasetEntities(datasetProvenanceInternal, fixed(project.entitiesProject)).generateOne
    val dataset2Modified = modifiedDatasetEntities(dataset2).generateOne

    Scenario("As a user I would like to find project's datasets by calling a REST endpoint") {

      Given("some data in the RDF Store")
      `data in the RDF store`(project, JsonLD.arr(dataset1.asJsonLD, dataset2.asJsonLD, dataset2Modified.asJsonLD))
      `wait for events to be processed`(project.id)

      And("the project exists in GitLab")
      `GET <gitlabApi>/projects/:path returning OK with`(project, withStatistics = true)

      When("user fetches project's datasets with GET knowledge-graph/projects/<project-name>/datasets")
      val projectDatasetsResponse = knowledgeGraphClient GET s"knowledge-graph/projects/${project.path}/datasets"

      Then("he should get OK response with project's datasets")
      projectDatasetsResponse.status shouldBe Ok
      val Right(foundDatasets) = projectDatasetsResponse.bodyAsJson.as[List[Json]]
      foundDatasets should contain theSameElementsAs List(briefJson(dataset1), briefJson(dataset2Modified))

      When("user then fetches details of the chosen dataset with the link from the response")
      val someDatasetDetailsLink = Random
        .shuffle(foundDatasets)
        .headOption
        .flatMap(_._links.get(Rel("details")))
        .getOrFail(message = "No link with rel 'details'")
      val datasetDetailsResponse = restClient GET someDatasetDetailsLink.toString

      Then("he should get OK response with dataset details")
      datasetDetailsResponse.status shouldBe Ok
      val foundDatasetDetails = datasetDetailsResponse.bodyAsJson
      val expectedDataset = List(dataset1, dataset2Modified)
        .find(dataset => someDatasetDetailsLink.value contains urlEncode(dataset.identifier.value))
        .getOrFail(message = "Returned 'details' link does not point to any dataset in the RDF store")
      findIdentifier(foundDatasetDetails) shouldBe expectedDataset.identifier

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
        .find(json => findIdentifier(json) == dataset2Modified.identifier)
        .getOrFail(s"No modified dataset with ${dataset2Modified.identifier} id")

      val modifiedDatasetInitialVersionLink = modifiedDataset2Json._links
        .get(Rel("initial-version"))
        .getOrFail("No link with rel 'initial-version'")
      val getInitialVersionResponse = restClient GET modifiedDatasetInitialVersionLink.toString

      Then("he should get OK response with project details")
      getInitialVersionResponse.status                     shouldBe Ok
      findIdentifier(getInitialVersionResponse.bodyAsJson) shouldBe dataset2.identifier
    }
  }

  Feature("GET knowledge-graph/datasets?query=<text> to find datasets with a free-text search") {

    Scenario("As a user I would like to be able to search for datasets by free-text search") {

      implicit val accessToken: AccessToken = accessTokens.generateOne

      val text = nonBlankStrings(minLength = 10).generateOne

      val dataset1                                     = datasetEntities(datasetProvenanceInternal).generateOne.makeTitleContaining(text)
      val dataset2                                     = datasetEntities(datasetProvenanceInternal).generateOne.makeDescContaining(text)
      val dataset3                                     = datasetEntities(datasetProvenanceInternal).generateOne.makeCreatorNameContaining(text)
      val dataset4Original                             = datasetEntities(datasetProvenanceInternal).generateOne.makeKeywordsContaining(text)
      val DatasetForkingResult(dataset4, dataset5Fork) = dataset4Original.forkProject()
      val dataset6WithoutText                          = datasetEntities(datasetProvenanceInternal).generateOne
      val dataset7Private = datasetEntities(datasetProvenanceInternal,
                                            projectEntities[model.projects.ForksCount.Zero](visibilityNonPublic)
      ).generateOne.makeTitleContaining(text)

      Given("some datasets with title, description, name and author containing some arbitrary chosen text")

      pushToStore(dataset1)
      pushToStore(dataset2)
      pushToStore(dataset3)
      pushToStore(dataset4)
      pushToStore(dataset5Fork)
      pushToStore(dataset6WithoutText)
      pushToStore(dataset7Private)

      When("user calls the GET knowledge-graph/datasets?query=<text>")
      val datasetsSearchResponse = knowledgeGraphClient GET s"knowledge-graph/datasets?query=${urlEncode(text.value)}"

      Then("he should get OK response with some matching datasets")
      datasetsSearchResponse.status shouldBe Ok

      val Right(foundDatasets) = datasetsSearchResponse.bodyAsJson.as[List[Json]]
      foundDatasets.flatMap(sortCreators) should contain theSameElementsAs List(
        searchResultJson(dataset1, 1, foundDatasets),
        searchResultJson(dataset2, 1, foundDatasets),
        searchResultJson(dataset3, 1, foundDatasets),
        searchResultJson(dataset4, 2, foundDatasets)
      ).flatMap(sortCreators)

      When("user calls the GET knowledge-graph/datasets?query=<text>&sort=title:asc")
      val searchSortedByName =
        knowledgeGraphClient GET s"knowledge-graph/datasets?query=${urlEncode(text.value)}&sort=title:asc"

      Then("he should get OK response with some matching datasets sorted by title ASC")
      searchSortedByName.status shouldBe Ok

      val Right(foundDatasetsSortedByName) = searchSortedByName.bodyAsJson.as[List[Json]]
      val datasetsSortedByName = List(
        searchResultJson(dataset1, 1, foundDatasetsSortedByName),
        searchResultJson(dataset2, 1, foundDatasetsSortedByName),
        searchResultJson(dataset3, 1, foundDatasetsSortedByName),
        searchResultJson(dataset4, 2, foundDatasetsSortedByName)
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
        searchResultJson(dataset1, 1, foundDatasetsWithoutPhrase),
        searchResultJson(dataset2, 1, foundDatasetsWithoutPhrase),
        searchResultJson(dataset3, 1, foundDatasetsWithoutPhrase),
        searchResultJson(dataset4, 2, foundDatasetsWithoutPhrase),
        searchResultJson(dataset6WithoutText, 1, foundDatasetsWithoutPhrase)
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

    Scenario("As an authenticated user I would like to be able to search for datasets by free-text search") {
      val user = authUsers.generateOne
      implicit val accessToken: AccessToken = user.accessToken

      Given("I am authenticated")
      `GET <gitlabApi>/user returning OK`(user)

      val text = nonBlankStrings(minLength = 10).generateOne

      val dataset1 = datasetEntities(datasetProvenanceInternal).generateOne.makeTitleContaining(text)

      val dataset2Private = datasetEntities(
        datasetProvenanceInternal,
        projectEntities[model.projects.ForksCount.Zero](Gen.oneOf(Visibility.Private, Visibility.Internal))
      ).generateOne.makeTitleContaining(text)

      val dataset3PrivateWithAccess = datasetEntities(
        datasetProvenanceInternal,
        projectEntities[model.projects.ForksCount.Zero](Gen.oneOf(Visibility.Private, Visibility.Internal))
          .map(_.copy(members = Set(personEntities.generateOne.copy(maybeGitLabId = user.id.some))))
      ).generateOne.makeTitleContaining(text)

      Given("some datasets with title, description, name and author containing some arbitrary chosen text")
      pushToStore(dataset1)
      pushToStore(dataset2Private)
      pushToStore(dataset3PrivateWithAccess)

      When("user calls the GET knowledge-graph/datasets?query=<text>")
      val datasetsSearchResponse =
        knowledgeGraphClient GET (s"knowledge-graph/datasets?query=${urlEncode(text.value)}&sort=title:asc", accessToken)

      Then("he should get OK response with some matching datasets")
      datasetsSearchResponse.status shouldBe Ok

      val Right(foundDatasets) = datasetsSearchResponse.bodyAsJson.as[List[Json]]
      foundDatasets.flatMap(sortCreators) should contain theSameElementsAs List(
        searchResultJson(dataset1, 1, foundDatasets),
        searchResultJson(dataset3PrivateWithAccess, 1, foundDatasets)
      ).flatMap(sortCreators)
    }

    def pushToStore(dataset: Dataset[Dataset.Provenance])(implicit
        accessToken:         AccessToken
    ): Unit = {
      val project = dataProjects(dataset.project).generateOne
      `data in the RDF store`(project, JsonLD.arr(dataset.asJsonLD, dataset.project.asJsonLD))
      `wait for events to be processed`(project.id)
      ()
    }
  }
}

object DatasetsResources {

  import ch.datascience.json.JsonOps._
  import ch.datascience.tinytypes.json.TinyTypeEncoders._

  def briefJson(dataset: Dataset[Dataset.Provenance])(implicit
      encoder:           Encoder[Dataset[Dataset.Provenance]]
  ): Json = encoder(dataset)

  implicit def datasetEncoder[P <: Dataset.Provenance](implicit
      provenanceEncoder: Encoder[P]
  ): Encoder[Dataset[P]] = Encoder.instance { dataset =>
    json"""{
      "identifier": ${dataset.identification.identifier.value},
      "versions": {
        "initial": ${dataset.provenance.initialVersion.value}
      },
      "title": ${dataset.identification.title.value},
      "name": ${dataset.identification.name.value},
      "images": ${dataset.additionalInfo.images.map(_.value)}
    }"""
      .deepMerge(
        _links(
          Rel("details")         -> Href(renkuResourcesUrl / "datasets" / dataset.identification.identifier),
          Rel("initial-version") -> Href(renkuResourcesUrl / "datasets" / dataset.provenance.initialVersion)
        )
      )
      .deepMerge(provenanceEncoder(dataset.provenance))
  }

  implicit def provenanceEncoder: Encoder[Dataset.Provenance] = Encoder.instance {
    case provenance: Dataset.Provenance.Modified => json"""{
        "derivedFrom": ${provenance.derivedFrom.value}
      }"""
    case provenance => json"""{
        "sameAs": ${provenance.topmostSameAs.value}
      }"""
  }

  def searchResultJson[P <: Dataset.Provenance](dataset:       Dataset[P],
                                                projectsCount: Int,
                                                actualResults: List[Json]
  ): Json = {
    val actualIdentifier = actualResults
      .findId(dataset.identification.title)
      .getOrElse(fail(s"No ${dataset.identification.title} dataset found among the results"))

    dataset.identifier shouldBe actualIdentifier

    json"""{
      "identifier": ${actualIdentifier.value},
      "title": ${dataset.identification.title.value},
      "name": ${dataset.identification.name.value},
      "published": ${dataset.provenance.creators -> dataset.provenance.date},
      "date": ${dataset.provenance.date.instant},
      "projectsCount": $projectsCount,
      "images": ${dataset.additionalInfo.images.map(_.value)},
      "keywords": ${dataset.additionalInfo.keywords.sorted.map(_.value)}
    }"""
      .addIfDefined("description" -> dataset.additionalInfo.maybeDescription)
      .deepMerge {
        _links(
          Rel("details") -> Href(renkuResourcesUrl / "datasets" / actualIdentifier)
        )
      }
  }

  private implicit def publishedEncoder[P <: Dataset.Provenance]: Encoder[(Set[Person], P#D)] =
    Encoder.instance {
      case (creators, DatePublished(date)) => json"""{
          "creator": ${creators.toList},
          "datePublished": $date
        }"""
      case (creators, _)                   => json"""{
          "creator": ${creators.toList}
        }"""
    }

  private implicit lazy val personEncoder: Encoder[Person] = Encoder.instance[Person] {
    case Person(name, maybeEmail, _, _) => json"""{
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

  implicit class JsonsOps(jsons: List[Json]) {

    def findId(title: Title): Option[Identifier] =
      jsons
        .find(_.hcursor.downField("title").as[String].fold(throw _, _ == title.toString))
        .map(_.hcursor.downField("identifier").as[Identifier].fold(throw _, identity))
  }

  def findIdentifier(json: Json): Identifier =
    json.hcursor.downField("identifier").as[Identifier].fold(throw _, identity)
}
