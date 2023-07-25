/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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
import io.circe.Json
import io.circe.literal._
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.data
import io.renku.graph.acceptancetests.data._
import io.renku.graph.acceptancetests.flows.TSProvisioning
import io.renku.graph.acceptancetests.tooling.{AcceptanceSpec, ApplicationServices}
import io.renku.graph.acceptancetests.tooling.TestReadabilityTools._
import io.renku.graph.model._
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.testentities.{::~, creatorUsernameUpdaterInternal}
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.http.client.UrlEncoder.urlEncode
import io.renku.http.rest.Links.Rel
import io.renku.http.server.EndpointTester._
import io.renku.http.server.security.model.AuthUser
import io.renku.tinytypes.json.TinyTypeDecoders._
import org.http4s.Status._
import org.scalatest.EitherValues

import scala.util.Random

class DatasetsResourcesSpec
    extends AcceptanceSpec
    with ApplicationServices
    with TSProvisioning
    with TSData
    with DatasetsApiEncoders
    with EitherValues {

  private val creator = authUsers.generateOne
  private val user    = authUsers.generateOne

  Feature("GET knowledge-graph/projects/<namespace>/<name>/datasets to find project's datasets") {

    val (dataset1 ::~ dataset2 ::~ dataset2Modified, testProject) =
      renkuProjectEntities(visibilityPublic, creatorGen = cliShapedPersons)
        .modify(removeMembers())
        .addDataset(datasetEntities(provenanceInternal(cliShapedPersons)))
        .addDatasetAndModification(
          datasetEntities(provenanceInternal(cliShapedPersons)),
          creatorGen = cliShapedPersons
        )
        .generateOne
    val creatorPerson = cliShapedPersons.generateOne
    val project =
      dataProjects(testProject)
        .map(replaceCreatorFrom(creatorPerson, creator.id))
        .map(addMemberFrom(creatorPerson, creator.id) >>> addMemberWithId(user.id))
        .generateOne

    Scenario("As a user I would like to find project's datasets by calling a REST endpoint") {

      Given("some data in the Triples Store")
      gitLabStub.addAuthenticated(creator)

      val commitId = commitIds.generateOne
      gitLabStub.setupProject(project, commitId)
      mockCommitDataOnTripleGenerator(project, toPayloadJsonLD(project), commitId)
      `data in the Triples Store`(project, commitId, creator.accessToken)

      When("user fetches project's datasets with GET knowledge-graph/projects/<project-name>/datasets")
      val projectDatasetsResponse = knowledgeGraphClient GET s"knowledge-graph/projects/${project.path}/datasets"

      Then("he should get OK response with project's datasets")
      projectDatasetsResponse.status shouldBe Ok
      val foundDatasets = projectDatasetsResponse.jsonBody.as[List[Json]].value
      foundDatasets should contain theSameElementsAs List(briefJson(dataset1, project.path),
                                                          briefJson(dataset2Modified, project.path)
      )

      When("user then fetches details of the chosen dataset with the link from the response")
      val someDatasetJson = Random.shuffle(foundDatasets).head
      val someDatasetDetailsLink = someDatasetJson._links
        .get(Rel("details"))
        .getOrFail(message = "No link with rel 'details'")
      val dsDetailsResponseStatus -> dsDetailsResponseBody = (restClient GET someDatasetDetailsLink.toString)
        .flatMap(response => response.as[Json].map(json => response.status -> json))
        .unsafeRunSync()

      Then("he should get OK response with dataset details")
      dsDetailsResponseStatus shouldBe Ok
      val expectedDataset = List(dataset1, dataset2Modified)
        .find(dataset => someDatasetDetailsLink.value contains urlEncode(dataset.identifier.value))
        .getOrFail(message = "Returned 'details' link does not point to any dataset in the Triples store")
      findIdentifier(dsDetailsResponseBody) shouldBe expectedDataset.identifier

      When("user is authenticated")
      gitLabStub.addAuthenticated(user)

      val datasetUsedInProjectLink = dsDetailsResponseBody.hcursor
        .downField("usedIn")
        .downArray
        ._links
        .get(Rel("project-details"))
        .getOrFail("No link with rel 'project-details'")

      dsDetailsResponseBody.hcursor
        .downField("project")
        ._links
        .get(Rel("project-details"))
        .getOrFail("No link with rel 'project-details'") shouldBe datasetUsedInProjectLink

      `data in the Triples Store`(project, commitId, user.accessToken)
      val getProjectResponseStatus -> getProjectResponseBody = restClient
        .GET(datasetUsedInProjectLink.toString, user.accessToken)
        .flatMap(response => response.as[Json].map(json => response.status -> json))
        .unsafeRunSync()

      Then("he should get OK response with project details")
      getProjectResponseStatus shouldBe Ok
      getProjectResponseBody   shouldBe fullJson(project)

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

      When("user fetches dataset's tags using the 'tags' link")
      val dsTagsLink = dsDetailsResponseBody.hcursor._links
        .get(Rel("tags"))
        .getOrFail("No link with rel 'tags' in the Dataset Details response")

      val (getTagsResponseStatus, getTagsJsons) = restClient
        .GET(dsTagsLink.toString, user.accessToken)
        .flatMap(response => response.as[List[Json]].map(json => response.status -> json))
        .unsafeRunSync()

      Then("he should get OK response with the dataset's tags")
      getTagsResponseStatus shouldBe Ok
      getTagsJsons
        .map(_.hcursor.downField("name").as[publicationEvents.Name])
        .sequence
        .fold(fail(_), identity) shouldBe {
        val events =
          if (expectedDataset == dataset1) dataset1.publicationEvents
          else List(dataset2, dataset2Modified).flatMap(_.publicationEvents)
        events.sortBy(_.startDate).reverse.map(_.name)
      }

      And("the 'tags' links on Project's Datasets and Dataset's Details resources are the same")
      dsTagsLink shouldBe someDatasetJson._links
        .get(Rel("tags"))
        .getOrFail("No link with rel 'tags' in the Project Datasets response")
    }

    Scenario("As a user I should not to be able to see project's datasets if I don't have rights to the project") {

      val (_, testPrivateProject) = renkuProjectEntities(visibilityPrivate, creatorGen = cliShapedPersons)
        .modify(removeMembers())
        .addDataset(datasetEntities(provenanceInternal(cliShapedPersons)))
        .generateOne
      val creatorPerson = cliShapedPersons.generateOne
      val privateProject =
        dataProjects(testPrivateProject)
          .map(replaceCreatorFrom(creatorPerson, creator.id))
          .map(addMemberFrom(creatorPerson, creator.id))
          .generateOne

      Given("there's a private project in KG")
      val commitId = commitIds.generateOne
      gitLabStub.addAuthenticated(creator, user)
      gitLabStub.setupProject(privateProject, commitId)
      mockCommitDataOnTripleGenerator(privateProject, toPayloadJsonLD(privateProject), commitId)
      `data in the Triples Store`(privateProject, commitId, creator.accessToken)

      When("there's an authenticated user who is not a member of the project")
      And("he fetches project's details")
      val response =
        knowledgeGraphClient.GET(s"knowledge-graph/projects/${privateProject.path}/datasets", user.accessToken)

      Then("he should get NOT_FOUND response")
      response.status shouldBe NotFound
    }
  }

  Feature("GET knowledge-graph/datasets?query=<text> to find datasets with a free-text search") {

    Scenario("As a user I would like to be able to search for datasets by free-text search") {

      gitLabStub.addAuthenticated(creator)

      val text = nonBlankStrings(minLength = 10).generateOne

      val (dataset1, testProject1) = renkuProjectEntities(visibilityPublic, creatorGen = cliShapedPersons)
        .modify(removeMembers())
        .addDataset(datasetEntities(provenanceInternal(cliShapedPersons)).modify(_.makeTitleContaining(text)))
        .generateOne
      val project1 = dataProjects(testProject1).map(addMemberWithId(creator.id)).generateOne

      val (dataset2, testProject2) = renkuProjectEntities(visibilityPublic, creatorGen = cliShapedPersons)
        .modify(removeMembers())
        .addDataset(datasetEntities(provenanceInternal(cliShapedPersons)).modify(_.makeDescContaining(text)))
        .generateOne
      val project2 = dataProjects(testProject2).map(addMemberWithId(creator.id)).generateOne
      val (dataset3, testProject3) = renkuProjectEntities(visibilityPublic, creatorGen = cliShapedPersons)
        .modify(removeMembers())
        .addDataset(
          datasetEntities(provenanceInternal(cliShapedPersons))
            .modify(_.makeCreatorNameContaining(text)(creatorUsernameUpdaterInternal(cliShapedPersons)))
        )
        .generateOne
      val project3 = dataProjects(testProject3).map(addMemberWithId(creator.id)).generateOne
      val (dataset4, testProject4 ::~ testProject4Fork) =
        renkuProjectEntities(visibilityPublic, creatorGen = cliShapedPersons)
          .addDataset(datasetEntities(provenanceInternal(cliShapedPersons)).modify(_.makeKeywordsContaining(text)))
          .forkOnce(creatorGen = cliShapedPersons)
          .generateOne
          .bimap(identity, _.bimap(removeMembers(), removeMembers()))
      val project4     = dataProjects(testProject4).map(addMemberWithId(creator.id)).generateOne
      val project4Fork = dataProjects(testProject4Fork).map(addMemberWithId(creator.id)).generateOne
      val (dataset5WithoutText, testProject5) = renkuProjectEntities(visibilityPublic, creatorGen = cliShapedPersons)
        .modify(removeMembers())
        .addDataset(datasetEntities(provenanceInternal(cliShapedPersons)))
        .generateOne
      val project5 = dataProjects(testProject5).map(addMemberWithId(creator.id)).generateOne
      val (_, testProject6Private) = renkuProjectEntities(visibilityPrivate, creatorGen = cliShapedPersons)
        .modify(removeMembers())
        .addDataset(datasetEntities(provenanceInternal(cliShapedPersons)).modify(_.makeTitleContaining(text)))
        .generateOne
      val project6CreatorPerson = cliShapedPersons.generateOne
      val project6Private = dataProjects(testProject6Private)
        .map(replaceCreatorFrom(project6CreatorPerson, creator.id))
        .map(addMemberFrom(project6CreatorPerson, creator.id))
        .generateOne

      Given("some datasets with title, description, name and author containing some arbitrary chosen text")

      pushToStore(project1, creator)
      pushToStore(project2, creator)
      pushToStore(project3, creator)
      pushToStore(project4, creator)
      pushToStore(project4Fork, creator)
      pushToStore(project5, creator)
      pushToStore(project6Private, creator)

      When("user calls the GET knowledge-graph/datasets?query=<text>")
      val dsSearchResponse = knowledgeGraphClient GET s"knowledge-graph/datasets?query=${urlEncode(text.value)}"

      Then("he should get OK response with some matching datasets")
      dsSearchResponse.status shouldBe Ok

      val foundDatasets = dsSearchResponse.jsonBody.as[List[Json]].value
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

      val foundDatasetsSortedByName = searchSortedByName.jsonBody.as[List[Json]].value
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
      val foundDatasetsPage = searchForPage.jsonBody.as[List[Json]].value
      foundDatasetsPage should {
        contain theSameElementsAs List(datasetsSortedByNameProj4Path(1)) or
          contain theSameElementsAs List(datasetsSortedByNameProj4ForkPath(1))
      }

      When("user calls the GET knowledge-graph/datasets?sort=name:asc")
      val searchWithoutPhrase = knowledgeGraphClient GET s"knowledge-graph/datasets?sort=title:asc"

      Then("he should get OK response with all the datasets")
      val foundDatasetsWithoutPhrase = searchWithoutPhrase.jsonBody.as[List[Json]].value
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

      Given("I am authenticated")
      gitLabStub.addAuthenticated(creator, user)

      val text = nonBlankStrings(minLength = 10).generateOne

      val (dataset1, testProject1) = renkuProjectEntities(visibilityPublic, creatorGen = cliShapedPersons)
        .modify(removeMembers())
        .addDataset(datasetEntities(provenanceInternal(cliShapedPersons)).modify(_.makeTitleContaining(text)))
        .generateOne
      val project1CreatorPerson = cliShapedPersons.generateOne
      val project1 = dataProjects(testProject1)
        .map(replaceCreatorFrom(project1CreatorPerson, creator.id))
        .map(addMemberWithId(user.id) >>> addMemberFrom(project1CreatorPerson, creator.id))
        .generateOne

      val (_, testProject2Private) = renkuProjectEntities(visibilityPrivate, creatorGen = cliShapedPersons)
        .modify(removeMembers())
        .addDataset(datasetEntities(provenanceInternal(cliShapedPersons)).modify(_.makeTitleContaining(text)))
        .generateOne
      val project2CreatorPerson = cliShapedPersons.generateOne
      val project2Private = dataProjects(testProject2Private)
        .map(replaceCreatorFrom(project2CreatorPerson, creator.id))
        .map(addMemberFrom(project2CreatorPerson, creator.id))
        .generateOne

      val (dataset3PrivateWithAccess, testProject3PrivateWithAccess) =
        renkuProjectEntities(visibilityPrivate, creatorGen = cliShapedPersons)
          .modify(removeMembers())
          .addDataset(datasetEntities(provenanceInternal(cliShapedPersons)).modify(_.makeTitleContaining(text)))
          .generateOne
      val project3CreatorPerson = cliShapedPersons.generateOne
      val project3PrivateWithAccess = dataProjects(testProject3PrivateWithAccess)
        .map(replaceCreatorFrom(project3CreatorPerson, creator.id))
        .map(addMemberWithId(user.id) >>> addMemberFrom(project3CreatorPerson, creator.id))
        .generateOne

      Given("some datasets with title, description, name and author containing some arbitrary chosen text")
      pushToStore(project1, creator)
      pushToStore(project2Private, creator)
      pushToStore(project3PrivateWithAccess, creator)

      When("user calls the GET knowledge-graph/datasets?query=<text>")
      val datasetsSearchResponse =
        knowledgeGraphClient GET (s"knowledge-graph/datasets?query=${urlEncode(text.value)}&sort=title:asc", user.accessToken)

      Then("he should get OK response with some matching datasets")
      datasetsSearchResponse.status shouldBe Ok

      val foundDatasets = datasetsSearchResponse.jsonBody.as[List[Json]].value
      foundDatasets should contain theSameElementsAs List(
        searchResultJson(dataset1, 1, project1.path, foundDatasets),
        searchResultJson(dataset3PrivateWithAccess, 1, project3PrivateWithAccess.path, foundDatasets)
      )
    }

    def pushToStore(project: data.Project, authUser: AuthUser) = {

      val commitId = commitIds.generateOne
      gitLabStub.setupProject(project, commitId)

      mockCommitDataOnTripleGenerator(project, toPayloadJsonLD(project), commitId)

      `data in the Triples Store`(project, commitId, authUser.accessToken)
    }
  }

  Feature("GET knowledge-graph/datasets/:id to find dataset details") {

    Scenario(
      "As an unauthenticated and unauthorised user I should be able to see details of a dataset on a public project"
    ) {
      val (dataset, testProject) = renkuProjectEntities(visibilityPublic, creatorGen = cliShapedPersons)
        .modify(removeMembers())
        .addDataset(datasetEntities(provenanceInternal(cliShapedPersons)))
        .generateOne

      val project = dataProjects(testProject).map(addMemberWithId(creator.id)).generateOne

      Given("some data in the Triples Store")
      gitLabStub.addAuthenticated(creator)
      val commitId = commitIds.generateOne
      gitLabStub.setupProject(project, commitId)
      mockCommitDataOnTripleGenerator(project, toPayloadJsonLD(project), commitId)
      `data in the Triples Store`(project, commitId, creator.accessToken)

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
      val (dataset, testProject) = renkuProjectEntities(visibilityPrivate, creatorGen = cliShapedPersons)
        .modify(removeMembers())
        .addDataset(datasetEntities(provenanceInternal(cliShapedPersons)))
        .generateOne

      val projectCreatorPerson = cliShapedPersons.generateOne
      val project = dataProjects(testProject)
        .map(replaceCreatorFrom(projectCreatorPerson, creator.id))
        .map(addMemberWithId(user.id) >>> addMemberFrom(projectCreatorPerson, creator.id))
        .generateOne

      Given("I am authenticated")
      gitLabStub.addAuthenticated(creator, user)

      Given("some data in the Triples Store")
      val commitId = commitIds.generateOne
      gitLabStub.setupProject(project, commitId)
      mockCommitDataOnTripleGenerator(project, toPayloadJsonLD(project), commitId)
      `data in the Triples Store`(project, commitId, creator.accessToken)

      When("an authenticated and authorised user fetches dataset details through GET knowledge-graph/datasets/:id")
      val detailsResponse =
        knowledgeGraphClient.GET(s"knowledge-graph/datasets/${dataset.identifier}", user.accessToken)

      Then("he should get OK response with the dataset details")
      detailsResponse.status            shouldBe Ok
      detailsResponse.jsonBody.as[Json] shouldBe a[Right[_, _]]

      When("unauthenticated user tries to fetch details of the same dataset")
      val unauthenticatedUser = authUsers.generateOne
      val unauthenticatedResponse =
        knowledgeGraphClient.GET(s"knowledge-graph/datasets/${dataset.identifier}", unauthenticatedUser.accessToken)

      Then("he should get Unauthorized response")
      unauthenticatedResponse.status shouldBe Unauthorized

      When("authenticated but unauthorised user tries to do the same")
      val unauthorisedUser = authUsers.generateOne
      gitLabStub.addAuthenticated(unauthorisedUser)
      val unauthorisedResponse =
        knowledgeGraphClient.GET(s"knowledge-graph/datasets/${dataset.identifier}", unauthorisedUser.accessToken)

      Then("he get NOT_FOUND response")
      unauthorisedResponse.status shouldBe NotFound
    }
  }
}
