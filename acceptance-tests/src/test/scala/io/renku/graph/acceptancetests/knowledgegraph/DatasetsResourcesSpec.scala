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

package io.renku.graph.acceptancetests.knowledgegraph

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.literal._
import io.circe.{Encoder, Json}
import io.renku.generators.CommonGraphGenerators.{accessTokens, authUsers}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.acceptancetests.data._
import io.renku.graph.acceptancetests.flows.RdfStoreProvisioning
import io.renku.graph.acceptancetests.tooling.GraphServices
import io.renku.graph.acceptancetests.tooling.TestReadabilityTools._
import io.renku.graph.model.datasets.{DatePublished, Identifier, ImageUri, Title}
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.graph.model.testentities.{::~, Dataset, Person}
import io.renku.graph.model.{projects, testentities}
import io.renku.http.client.AccessToken
import io.renku.http.client.UrlEncoder.urlEncode
import io.renku.http.rest.Links.{Href, Rel, _links}
import io.renku.http.server.EndpointTester._
import io.renku.jsonld.syntax._
import io.renku.tinytypes.json.TinyTypeDecoders._
import org.http4s.Status._
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should

import scala.util.Random

class DatasetsResourcesSpec
    extends AnyFeatureSpec
    with GivenWhenThen
    with GraphServices
    with RdfStoreProvisioning
    with RdfStoreData
    with DatasetsResources {

  Feature("GET knowledge-graph/projects/<namespace>/<name>/datasets to find project's datasets") {

    val user = authUsers.generateOne
    implicit val accessToken: AccessToken = user.accessToken

    val (dataset1 ::~ dataset2 ::~ dataset2Modified, testEntitiesProject) = projectEntities(visibilityPublic)
      .addDataset(datasetEntities(provenanceInternal))
      .addDatasetAndModification(datasetEntities(provenanceInternal))
      .generateOne
    val project = dataProjects(testEntitiesProject).generateOne

    Scenario("As a user I would like to find project's datasets by calling a REST endpoint") {

      Given("some data in the RDF Store")
      `data in the RDF store`(project, testEntitiesProject.asJsonLD)
      `wait for events to be processed`(project.id)

      When("user fetches project's datasets with GET knowledge-graph/projects/<project-name>/datasets")
      val projectDatasetsResponse = knowledgeGraphClient GET s"knowledge-graph/projects/${project.path}/datasets"

      Then("he should get OK response with project's datasets")
      projectDatasetsResponse.status shouldBe Ok
      val Right(foundDatasets) = projectDatasetsResponse.jsonBody.as[List[Json]]
      foundDatasets should contain theSameElementsAs List(briefJson(dataset1, project.path),
                                                          briefJson(dataset2Modified, project.path)
      )

      When("user then fetches details of the chosen dataset with the link from the response")
      val someDatasetDetailsLink = Random
        .shuffle(foundDatasets)
        .headOption
        .flatMap(_._links.get(Rel("details")))
        .getOrFail(message = "No link with rel 'details'")
      val datasetDetailsResponse = (restClient GET someDatasetDetailsLink.toString)
        .flatMap(response => response.as[Json].map(json => response.status -> json))
        .unsafeRunSync()

      Then("he should get OK response with dataset details")
      datasetDetailsResponse._1 shouldBe Ok
      val foundDatasetDetails = datasetDetailsResponse._2
      val expectedDataset = List(dataset1, dataset2Modified)
        .find(dataset => someDatasetDetailsLink.value contains urlEncode(dataset.identifier.value))
        .getOrFail(message = "Returned 'details' link does not point to any dataset in the RDF store")
      findIdentifier(foundDatasetDetails) shouldBe expectedDataset.identifier

      When("user is authenticated")
      `GET <gitlabApi>/user returning OK`(user)

      val datasetUsedInProjectLink = foundDatasetDetails.hcursor
        .downField("usedIn")
        .downArray
        ._links
        .get(Rel("project-details"))
        .getOrFail("No link with rel 'project-details'")

      foundDatasetDetails.hcursor
        .downField("project")
        ._links
        .get(Rel("project-details"))
        .getOrFail("No link with rel 'project-details'") shouldBe datasetUsedInProjectLink

      val getProjectResponse = restClient
        .GET(datasetUsedInProjectLink.toString, user.accessToken)
        .flatMap(response => response.as[Json].map(json => response.status -> json))
        .unsafeRunSync()

      Then("he should get OK response with project details")
      getProjectResponse._1 shouldBe Ok
      getProjectResponse._2 shouldBe ProjectsResources.fullJson(project)

      When("user fetches initial version of the modified dataset with the link from the response")
      val modifiedDataset2Json = foundDatasets
        .find(json => findIdentifier(json) == dataset2Modified.identifier)
        .getOrFail(s"No modified dataset with ${dataset2Modified.identifier} id")

      val modifiedDatasetInitialVersionLink = modifiedDataset2Json._links
        .get(Rel("initial-version"))
        .getOrFail("No link with rel 'initial-version'")
      val getInitialVersionResponse = (restClient GET modifiedDatasetInitialVersionLink.toString)
        .flatMap(response => response.as[Json].map(json => response.status -> json))
        .unsafeRunSync()

      Then("he should get OK response with project details")
      getInitialVersionResponse._1                 shouldBe Ok
      findIdentifier(getInitialVersionResponse._2) shouldBe dataset2.identifier
    }

    Scenario("As a user I should not to be able to see project's datasets if I don't have rights to the project") {

      val (_, testEntitiesNonPublicProject) = projectEntities(visibilityNonPublic)
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne
      val nonPublicProject = dataProjects(testEntitiesNonPublicProject).generateOne

      Given("there's a non-public project in KG")
      `data in the RDF store`(nonPublicProject, testEntitiesNonPublicProject.asJsonLD)
      `wait for events to be processed`(nonPublicProject.id)

      When("there's an authenticated user who is not a member of the project")
      val nonMemberAccessToken = accessTokens.generateOne
      `GET <gitlabApi>/user returning OK`()(nonMemberAccessToken)

      And("he fetches project's details")
      val projectDatasetsResponseForNonMember =
        knowledgeGraphClient.GET(s"knowledge-graph/projects/${nonPublicProject.path}/datasets", nonMemberAccessToken)

      Then("he should get NOT_FOUND response")
      projectDatasetsResponseForNonMember.status shouldBe NotFound
    }
  }

  Feature("GET knowledge-graph/datasets?query=<text> to find datasets with a free-text search") {

    Scenario("As a user I would like to be able to search for datasets by free-text search") {

      implicit val accessToken: AccessToken = accessTokens.generateOne

      val text = nonBlankStrings(minLength = 10).generateOne

      val (dataset1, project1) = projectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceInternal).modify(_.makeTitleContaining(text)))
        .generateOne
      val (dataset2, project2) = projectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceInternal).modify(_.makeDescContaining(text)))
        .generateOne
      val (dataset3, project3) = projectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceInternal).modify(_.makeCreatorNameContaining(text)))
        .generateOne
      val (dataset4, project4 ::~ project4Fork) = projectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceInternal).modify(_.makeKeywordsContaining(text)))
        .forkOnce()
        .generateOne
      val (dataset5WithoutText, project5) = projectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne
      val (_, project6Private) = projectEntities(visibilityNonPublic)
        .addDataset(datasetEntities(provenanceInternal).modify(_.makeTitleContaining(text)))
        .generateOne
      Given("some datasets with title, description, name and author containing some arbitrary chosen text")

      pushToStore(project1)
      pushToStore(project2)
      pushToStore(project3)
      pushToStore(project4)
      pushToStore(project4Fork)
      pushToStore(project5)
      pushToStore(project6Private)

      When("user calls the GET knowledge-graph/datasets?query=<text>")
      val datasetsSearchResponse = knowledgeGraphClient GET s"knowledge-graph/datasets?query=${urlEncode(text.value)}"

      Then("he should get OK response with some matching datasets")
      datasetsSearchResponse.status shouldBe Ok

      val Right(foundDatasets) = datasetsSearchResponse.jsonBody.as[List[Json]]
      foundDatasets.flatMap(sortCreators) should contain theSameElementsAs List(
        searchResultJson(dataset1, 1, project1.path, foundDatasets),
        searchResultJson(dataset2, 1, project2.path, foundDatasets),
        searchResultJson(dataset3, 1, project3.path, foundDatasets),
        searchResultJson(dataset4, 2, project4.path, foundDatasets)
      ).flatMap(sortCreators)

      When("user calls the GET knowledge-graph/datasets?query=<text>&sort=title:asc")
      val searchSortedByName =
        knowledgeGraphClient GET s"knowledge-graph/datasets?query=${urlEncode(text.value)}&sort=title:asc"

      Then("he should get OK response with some matching datasets sorted by title ASC")
      searchSortedByName.status shouldBe Ok

      val Right(foundDatasetsSortedByName) = searchSortedByName.jsonBody.as[List[Json]]
      val datasetsSortedByName = List(
        searchResultJson(dataset1, 1, project1.path, foundDatasetsSortedByName),
        searchResultJson(dataset2, 1, project2.path, foundDatasetsSortedByName),
        searchResultJson(dataset3, 1, project3.path, foundDatasetsSortedByName),
        searchResultJson(dataset4, 2, project4.path, foundDatasetsSortedByName)
      ).flatMap(sortCreators)
        .sortBy(_.hcursor.downField("title").as[String].getOrElse(fail("No 'title' property found")))
      foundDatasetsSortedByName.flatMap(sortCreators) shouldBe datasetsSortedByName

      When("user calls the GET knowledge-graph/datasets?query=<text>&sort=title:asc&page=2&per_page=1")
      val searchForPage =
        knowledgeGraphClient GET s"knowledge-graph/datasets?query=${urlEncode(text.value)}&sort=title:asc&page=2&per_page=1"

      Then("he should get OK response with the dataset from the requested page")
      val Right(foundDatasetsPage) = searchForPage.jsonBody.as[List[Json]]
      foundDatasetsPage.flatMap(sortCreators) should contain theSameElementsAs List(datasetsSortedByName(1))
        .flatMap(sortCreators)

      When("user calls the GET knowledge-graph/datasets?sort=name:asc")
      val searchWithoutPhrase = knowledgeGraphClient GET s"knowledge-graph/datasets?sort=title:asc"

      Then("he should get OK response with all the datasets")
      val Right(foundDatasetsWithoutPhrase) = searchWithoutPhrase.jsonBody.as[List[Json]]
      foundDatasetsWithoutPhrase.flatMap(sortCreators) should contain allElementsOf List(
        searchResultJson(dataset1, 1, project1.path, foundDatasetsWithoutPhrase),
        searchResultJson(dataset2, 1, project2.path, foundDatasetsWithoutPhrase),
        searchResultJson(dataset3, 1, project3.path, foundDatasetsWithoutPhrase),
        searchResultJson(dataset4, 2, project4.path, foundDatasetsWithoutPhrase),
        searchResultJson(dataset5WithoutText, 1, project5.path, foundDatasetsWithoutPhrase)
      ).flatMap(sortCreators)
        .sortBy(_.hcursor.downField("title").as[String].getOrElse(fail("No 'title' property found")))

      When("user uses the response header link with the rel='first'")
      val firstPageLink     = searchForPage.headerLink(rel = "first")
      val firstPageResponse = restClient GET firstPageLink

      Then("he should get OK response with the datasets from the first page")
      val foundFirstPage = firstPageResponse.flatMap(_.as[List[Json]]).unsafeRunSync()
      foundFirstPage.flatMap(sortCreators) should contain theSameElementsAs List(datasetsSortedByName.head)
        .flatMap(sortCreators)

      When("user uses 'details' link of one of the found datasets")
      val someDatasetJson = Random.shuffle(foundDatasets).head

      val detailsLinkResponse = (restClient GET someDatasetJson._links
        .get(Rel("details"))
        .getOrFail(message = "No link with rel 'details'")
        .toString).flatMap(response => response.as[Json].map(json => response.status -> json)).unsafeRunSync()

      detailsLinkResponse._1 shouldBe Ok

      Then("he should get the same result as he'd call the knowledge-graph/datasets/:id endpoint directly")
      val datasetDetailsResponse =
        knowledgeGraphClient GET s"knowledge-graph/datasets/${urlEncode(findIdentifier(someDatasetJson).toString)}"
      detailsLinkResponse._2 shouldBe datasetDetailsResponse.jsonBody
    }

    Scenario("As an authenticated user I would like to be able to search for datasets by free-text search") {
      val user = authUsers.generateOne
      implicit val accessToken: AccessToken = user.accessToken

      Given("I am authenticated")
      `GET <gitlabApi>/user returning OK`(user)

      val text = nonBlankStrings(minLength = 10).generateOne

      val (dataset1, project1) = projectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceInternal).modify(_.makeTitleContaining(text)))
        .generateOne

      val (_, project2Private) = projectEntities(visibilityNonPublic)
        .addDataset(datasetEntities(provenanceInternal).modify(_.makeTitleContaining(text)))
        .generateOne

      val (dataset3PrivateWithAccess, project3PrivateWithAccess) = projectEntities(visibilityNonPublic)
        .map(_.copy(members = Set(personEntities.generateOne.copy(maybeGitLabId = user.id.some))))
        .addDataset(datasetEntities(provenanceInternal).modify(_.makeTitleContaining(text)))
        .generateOne

      Given("some datasets with title, description, name and author containing some arbitrary chosen text")
      pushToStore(project1)
      pushToStore(project2Private)
      pushToStore(project3PrivateWithAccess)

      When("user calls the GET knowledge-graph/datasets?query=<text>")
      val datasetsSearchResponse =
        knowledgeGraphClient GET (s"knowledge-graph/datasets?query=${urlEncode(text.value)}&sort=title:asc", accessToken)

      Then("he should get OK response with some matching datasets")
      datasetsSearchResponse.status shouldBe Ok

      val Right(foundDatasets) = datasetsSearchResponse.jsonBody.as[List[Json]]
      foundDatasets.flatMap(sortCreators) should contain theSameElementsAs List(
        searchResultJson(dataset1, 1, project1.path, foundDatasets),
        searchResultJson(dataset3PrivateWithAccess, 1, project3PrivateWithAccess.path, foundDatasets)
      ).flatMap(sortCreators)
    }

    def pushToStore(project: testentities.Project)(implicit accessToken: AccessToken): Unit = {
      `data in the RDF store`(dataProjects(project).generateOne, project.asJsonLD)
      ()
    }
  }
}

trait DatasetsResources {
  self: GraphServices with should.Matchers =>

  import io.renku.json.JsonOps._
  import io.renku.tinytypes.json.TinyTypeEncoders._

  def briefJson(dataset: Dataset[Dataset.Provenance], projectPath: projects.Path)(implicit
      encoder:           Encoder[(Dataset[Dataset.Provenance], projects.Path)]
  ): Json = encoder(dataset -> projectPath)

  implicit def datasetEncoder[P <: Dataset.Provenance](implicit
      provenanceEncoder: Encoder[P]
  ): Encoder[(Dataset[P], projects.Path)] = Encoder.instance { case (dataset, projectPath) =>
    json"""{
      "identifier": ${dataset.identification.identifier.value},
      "versions": {
        "initial": ${dataset.provenance.initialVersion.value}
      },
      "title": ${dataset.identification.title.value},
      "name": ${dataset.identification.name.value},
      "images": ${dataset.additionalInfo.images -> projectPath}
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
                                                projectPath:   projects.Path,
                                                actualResults: List[Json]
  ): Json = {
    val actualIdentifier = actualResults
      .findId(dataset.identification.title)
      .getOrElse(fail(s"No ${dataset.identification.title} dataset found among the results"))

    dataset.identification.identifier shouldBe actualIdentifier

    json"""{
      "identifier": ${actualIdentifier.value},
      "title": ${dataset.identification.title.value},
      "name": ${dataset.identification.name.value},
      "published": ${dataset.provenance.creators -> dataset.provenance.date},
      "date": ${dataset.provenance.date.instant},
      "projectsCount": $projectsCount,
      "images": ${dataset.additionalInfo.images -> projectPath},
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
      case (creators, _) => json"""{
          "creator": ${creators.toList}
        }"""
    }

  private implicit lazy val personEncoder: Encoder[Person] = Encoder.instance[Person] {
    case Person(name, maybeEmail, _, _) => json"""{
      "name": $name
    }""" addIfDefined ("email" -> maybeEmail)
  }

  private implicit lazy val imagesEncoder: Encoder[(List[ImageUri], projects.Path)] =
    Encoder.instance[(List[ImageUri], projects.Path)] { case (images, exemplarProjectPath) =>
      Json.arr(images.map {
        case uri: ImageUri.Relative => json"""{
            "location": $uri,
            "_links": [{
              "rel": "view",
              "href": ${s"$gitLabUrl/$exemplarProjectPath/raw/master/$uri"}
            }]
          }"""
        case uri: ImageUri.Absolute => json"""{
            "location": $uri,
            "_links": [{
              "rel": "view",
              "href": $uri
            }]
          }"""
      }: _*)
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
