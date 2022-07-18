/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

import cats.data.NonEmptyList
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.literal._
import io.circe.{Encoder, Json}
import io.renku.generators.CommonGraphGenerators.{accessTokens, authUsers}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.acceptancetests.data._
import io.renku.graph.acceptancetests.flows.TSProvisioning
import io.renku.graph.acceptancetests.tooling.GraphServices
import io.renku.graph.acceptancetests.tooling.TestReadabilityTools._
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.datasets.{DatePublished, Identifier, ImageUri, Title}
import io.renku.graph.model.projects.Visibility
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
    with TSProvisioning
    with TSData
    with DatasetsResources {

  Feature("GET knowledge-graph/projects/<namespace>/<name>/datasets to find project's datasets") {

    val user = authUsers.generateOne
    implicit val accessToken: AccessToken = user.accessToken

    val (dataset1 ::~ dataset2 ::~ dataset2Modified, testEntitiesProject) = renkuProjectEntities(visibilityPublic)
      .addDataset(datasetEntities(provenanceInternal))
      .addDatasetAndModification(datasetEntities(provenanceInternal))
      .generateOne
    val project = dataProjects(testEntitiesProject).generateOne

    Scenario("As a user I would like to find project's datasets by calling a REST endpoint") {

      Given("some data in the Triples Store")
      val commitId = commitIds.generateOne
      mockDataOnGitLabAPIs(project, testEntitiesProject.asJsonLD, commitId)
      `data in the Triples Store`(project, commitId)

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
        .getOrFail(message = "Returned 'details' link does not point to any dataset in the Triples store")
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
      getProjectResponse._2 shouldBe fullJson(project)

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

      val (_, testEntitiesPrivateProject) = renkuProjectEntities(fixed(Visibility.Private))
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne
      val privateProject = dataProjects(testEntitiesPrivateProject).generateOne

      Given("there's a non-public project in KG")
      val commitId = commitIds.generateOne
      mockDataOnGitLabAPIs(privateProject, testEntitiesProject.asJsonLD, commitId)
      `data in the Triples Store`(privateProject, commitId)

      When("there's an authenticated user who is not a member of the project")
      val nonMemberAccessToken = accessTokens.generateOne
      `GET <gitlabApi>/user returning OK`()(nonMemberAccessToken)

      And("he fetches project's details")
      val projectDatasetsResponseForNonMember =
        knowledgeGraphClient.GET(s"knowledge-graph/projects/${privateProject.path}/datasets", nonMemberAccessToken)

      Then("he should get NOT_FOUND response")
      projectDatasetsResponseForNonMember.status shouldBe NotFound
    }
  }

  Feature("GET knowledge-graph/datasets?query=<text> to find datasets with a free-text search") {

    Scenario("As a user I would like to be able to search for datasets by free-text search") {

      implicit val accessToken: AccessToken = accessTokens.generateOne

      val text = nonBlankStrings(minLength = 10).generateOne

      val (dataset1, project1) = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceInternal).modify(_.makeTitleContaining(text)))
        .generateOne
      val (dataset2, project2) = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceInternal).modify(_.makeDescContaining(text)))
        .generateOne
      val (dataset3, project3) = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceInternal).modify(_.makeCreatorNameContaining(text)))
        .generateOne
      val (dataset4, project4 ::~ project4Fork) = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceInternal).modify(_.makeKeywordsContaining(text)))
        .forkOnce()
        .generateOne
      val (dataset5WithoutText, project5) = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne
      val (_, project6Private) = renkuProjectEntities(visibilityNonPublic)
        .addDataset(datasetEntities(provenanceInternal).modify(_.makeTitleContaining(text)))
        .generateOne
      Given("some datasets with title, description, name and author containing some arbitrary chosen text")

      val dataProject1     = pushToStore(project1)
      val dataProject2     = pushToStore(project2)
      val dataProject3     = pushToStore(project3)
      val dataProject4     = pushToStore(project4)
      val dataProject4Fork = pushToStore(project4Fork)
      val dataProject5     = pushToStore(project5)
      val dataProject6     = pushToStore(project6Private)

      `wait for events to be processed`(dataProject1.id)
      `wait for events to be processed`(dataProject2.id)
      `wait for events to be processed`(dataProject3.id)
      `wait for events to be processed`(dataProject4.id)
      `wait for events to be processed`(dataProject4Fork.id)
      `wait for events to be processed`(dataProject5.id)
      `wait for events to be processed`(dataProject6.id)

      When("user calls the GET knowledge-graph/datasets?query=<text>")
      val datasetsSearchResponse = knowledgeGraphClient GET s"knowledge-graph/datasets?query=${urlEncode(text.value)}"

      Then("he should get OK response with some matching datasets")
      datasetsSearchResponse.status shouldBe Ok

      val Right(foundDatasets) = datasetsSearchResponse.jsonBody.as[List[Json]]
      foundDatasets should {
        contain theSameElementsAs List(
          searchResultJson(dataset1, 1, project1.path, foundDatasets),
          searchResultJson(dataset2, 1, project2.path, foundDatasets),
          searchResultJson(dataset3, 1, project3.path, foundDatasets),
          searchResultJson(dataset4, 2, project4.path, foundDatasets)
        ) or contain theSameElementsAs List(
          searchResultJson(dataset1, 1, project1.path, foundDatasets),
          searchResultJson(dataset2, 1, project2.path, foundDatasets),
          searchResultJson(dataset3, 1, project3.path, foundDatasets),
          searchResultJson(dataset4, 2, project4Fork.path, foundDatasets)
        )
      }

      When("user calls the GET knowledge-graph/datasets?query=<text>&sort=title:asc")
      val searchSortedByName =
        knowledgeGraphClient GET s"knowledge-graph/datasets?query=${urlEncode(text.value)}&sort=title:asc"

      Then("he should get OK response with some matching datasets sorted by title ASC")
      searchSortedByName.status shouldBe Ok

      val Right(foundDatasetsSortedByName) = searchSortedByName.jsonBody.as[List[Json]]
      val datasetsSortedByNameProj4Path = List(
        searchResultJson(dataset1, 1, project1.path, foundDatasetsSortedByName),
        searchResultJson(dataset2, 1, project2.path, foundDatasetsSortedByName),
        searchResultJson(dataset3, 1, project3.path, foundDatasetsSortedByName),
        searchResultJson(dataset4, 2, project4.path, foundDatasetsSortedByName)
      ).sortBy(_.hcursor.downField("title").as[String].getOrElse(fail("No 'title' property found")))
      val datasetsSortedByNameProj4ForkPath = List(
        searchResultJson(dataset1, 1, project1.path, foundDatasetsSortedByName),
        searchResultJson(dataset2, 1, project2.path, foundDatasetsSortedByName),
        searchResultJson(dataset3, 1, project3.path, foundDatasetsSortedByName),
        searchResultJson(dataset4, 2, project4Fork.path, foundDatasetsSortedByName)
      ).sortBy(_.hcursor.downField("title").as[String].getOrElse(fail("No 'title' property found")))

      foundDatasetsSortedByName should {
        be(datasetsSortedByNameProj4Path) or be(datasetsSortedByNameProj4ForkPath)
      }

      When("user calls the GET knowledge-graph/datasets?query=<text>&sort=title:asc&page=2&per_page=1")
      val searchForPage =
        knowledgeGraphClient GET s"knowledge-graph/datasets?query=${urlEncode(text.value)}&sort=title:asc&page=2&per_page=1"

      Then("he should get OK response with the dataset from the requested page")
      val Right(foundDatasetsPage) = searchForPage.jsonBody.as[List[Json]]
      foundDatasetsPage should {
        contain theSameElementsAs List(datasetsSortedByNameProj4Path(1)) or
          contain theSameElementsAs List(datasetsSortedByNameProj4ForkPath(1))
      }

      When("user calls the GET knowledge-graph/datasets?sort=name:asc")
      val searchWithoutPhrase = knowledgeGraphClient GET s"knowledge-graph/datasets?sort=title:asc"

      Then("he should get OK response with all the datasets")
      val Right(foundDatasetsWithoutPhrase) = searchWithoutPhrase.jsonBody.as[List[Json]]
      foundDatasetsWithoutPhrase should {
        contain allElementsOf List(
          searchResultJson(dataset1, 1, project1.path, foundDatasetsWithoutPhrase),
          searchResultJson(dataset2, 1, project2.path, foundDatasetsWithoutPhrase),
          searchResultJson(dataset3, 1, project3.path, foundDatasetsWithoutPhrase),
          searchResultJson(dataset4, 2, project4.path, foundDatasetsWithoutPhrase),
          searchResultJson(dataset5WithoutText, 1, project5.path, foundDatasetsWithoutPhrase)
        ).sortBy(_.hcursor.downField("title").as[String].getOrElse(fail("No 'title' property found"))) or
          contain allElementsOf List(
            searchResultJson(dataset1, 1, project1.path, foundDatasetsWithoutPhrase),
            searchResultJson(dataset2, 1, project2.path, foundDatasetsWithoutPhrase),
            searchResultJson(dataset3, 1, project3.path, foundDatasetsWithoutPhrase),
            searchResultJson(dataset4, 2, project4Fork.path, foundDatasetsWithoutPhrase),
            searchResultJson(dataset5WithoutText, 1, project5.path, foundDatasetsWithoutPhrase)
          ).sortBy(_.hcursor.downField("title").as[String].getOrElse(fail("No 'title' property found")))
      }

      When("user uses the response header link with the rel='first'")
      val firstPageLink     = searchForPage.headerLink(rel = "first")
      val firstPageResponse = restClient GET firstPageLink

      Then("he should get OK response with the datasets from the first page")
      val foundFirstPage = firstPageResponse.flatMap(_.as[List[Json]]).unsafeRunSync()
      foundFirstPage should {
        contain theSameElementsAs List(datasetsSortedByNameProj4Path.head) or
          contain theSameElementsAs List(datasetsSortedByNameProj4ForkPath.head)
      }

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

    Scenario("As an authenticated user I would like to be able to search for datasets using free-text search") {
      val user = authUsers.generateOne
      implicit val accessToken: AccessToken = user.accessToken

      Given("I am authenticated")
      `GET <gitlabApi>/user returning OK`(user)

      val text = nonBlankStrings(minLength = 10).generateOne

      val (dataset1, project1) = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceInternal).modify(_.makeTitleContaining(text)))
        .generateOne

      val (_, project2Private) = renkuProjectEntities(fixed(Visibility.Private))
        .addDataset(datasetEntities(provenanceInternal).modify(_.makeTitleContaining(text)))
        .generateOne

      val (dataset3PrivateWithAccess, project3PrivateWithAccess) = renkuProjectEntities(fixed(Visibility.Private))
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
      foundDatasets should contain theSameElementsAs List(
        searchResultJson(dataset1, 1, project1.path, foundDatasets),
        searchResultJson(dataset3PrivateWithAccess, 1, project3PrivateWithAccess.path, foundDatasets)
      )
    }

    def pushToStore(project: testentities.RenkuProject)(implicit accessToken: AccessToken): Project = {
      val dataProject = dataProjects(project).generateOne
      val commitId    = commitIds.generateOne
      mockDataOnGitLabAPIs(dataProject, project.asJsonLD, commitId)
      `data in the Triples Store`(dataProject, commitId)
      dataProject
    }
  }

  Feature("GET knowledge-graph/datasets/:id to find dataset details") {

    Scenario(
      "As an unauthenticated and unauthorised user I should be able to see details of dataset on a public project"
    ) {
      implicit val accessToken: AccessToken = accessTokens.generateOne

      val (dataset, testEntitiesProject) = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne

      val project = dataProjects(testEntitiesProject).generateOne

      Given("some data in the Triples Store")
      val commitId = commitIds.generateOne
      mockDataOnGitLabAPIs(project, testEntitiesProject.asJsonLD, commitId)
      `data in the Triples Store`(project, commitId)

      When("user fetches dataset details with GET knowledge-graph/datasets/:id")
      val detailsResponse = knowledgeGraphClient GET s"knowledge-graph/datasets/${dataset.identifier}"

      Then("he should get OK response with dataset's details")
      detailsResponse.status            shouldBe Ok
      detailsResponse.jsonBody.as[Json] shouldBe a[Right[_, _]]
    }

    Scenario(
      "As an authenticated and authorised user I should be able to see details of a dataset on a private project " +
        "and not see them if either not authorised or not authenticated"
    ) {
      val user = authUsers.generateOne
      implicit val accessToken: AccessToken = user.accessToken

      val (dataset, testEntitiesProject) = renkuProjectEntities(fixed(Visibility.Private))
        .map(_.copy(members = Set(personEntities.generateOne.copy(maybeGitLabId = user.id.some))))
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne

      val project = dataProjects(testEntitiesProject).generateOne

      Given("I am authenticated")
      `GET <gitlabApi>/user returning OK`(user)

      Given("some data in the Triples Store")
      val commitId = commitIds.generateOne
      mockDataOnGitLabAPIs(project, testEntitiesProject.asJsonLD, commitId)
      `data in the Triples Store`(project, commitId)
      `wait for events to be processed`(project.id)

      When("an authenticated and authorised user fetches dataset details through GET knowledge-graph/datasets/:id")
      val detailsResponse = knowledgeGraphClient.GET(s"knowledge-graph/datasets/${dataset.identifier}", accessToken)

      Then("he should get OK response with the dataset details")
      detailsResponse.status            shouldBe Ok
      detailsResponse.jsonBody.as[Json] shouldBe a[Right[_, _]]

      When("unauthenticated user tries to fetch details of the same dataset")
      val unauthenticatedUser = authUsers.generateOne
      `GET <gitlabApi>/user returning NOT_FOUND`(unauthenticatedUser)
      val unauthenticatedResponse =
        knowledgeGraphClient.GET(s"knowledge-graph/datasets/${dataset.identifier}", unauthenticatedUser.accessToken)

      Then("he should get Unauthorized response")
      unauthenticatedResponse.status shouldBe Unauthorized

      When("authenticated but unauthorised user tries to do the same")
      val unauthorisedUser = authUsers.generateOne
      `GET <gitlabApi>/user returning OK`(unauthorisedUser)
      val unauthorisedResponse =
        knowledgeGraphClient.GET(s"knowledge-graph/datasets/${dataset.identifier}", unauthorisedUser.accessToken)

      Then("he get NOT_FOUND response")
      unauthorisedResponse.status shouldBe NotFound
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
        "initial": ${dataset.provenance.originalIdentifier.value}
      },
      "title": ${dataset.identification.title.value},
      "name": ${dataset.identification.name.value},
      "images": ${dataset.additionalInfo.images -> projectPath}
    }"""
      .deepMerge(
        _links(
          Rel("details")         -> Href(renkuApiUrl / "datasets" / dataset.identification.identifier),
          Rel("initial-version") -> Href(renkuApiUrl / "datasets" / dataset.provenance.originalIdentifier)
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
      "keywords": ${dataset.additionalInfo.keywords.sorted.map(_.value)},
      "images": ${dataset.additionalInfo.images -> projectPath}
    }"""
      .addIfDefined("description" -> dataset.additionalInfo.maybeDescription)
      .deepMerge {
        _links(
          Rel("details") -> Href(renkuApiUrl / "datasets" / actualIdentifier)
        )
      }
  }

  private implicit def publishedEncoder[P <: Dataset.Provenance]: Encoder[(NonEmptyList[Person], P#D)] =
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
    case Person(name, maybeEmail, _, _, _) => json"""{
      "name": $name
    }""" addIfDefined ("email" -> maybeEmail)
  }

  private implicit lazy val imagesEncoder: Encoder[(List[ImageUri], projects.Path)] =
    Encoder.instance[(List[ImageUri], projects.Path)] { case (images, exemplarProjectPath) =>
      Json.arr(images.map {
        case uri: ImageUri.Relative => json"""{
            "_links": [{
              "rel": "view",
              "href": ${s"$gitLabUrl/$exemplarProjectPath/raw/master/$uri"}
            }],
            "location": $uri
          }"""
        case uri: ImageUri.Absolute => json"""{
            "_links": [{
              "rel": "view",
              "href": $uri
            }],
            "location": $uri
          }"""
      }: _*)
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
