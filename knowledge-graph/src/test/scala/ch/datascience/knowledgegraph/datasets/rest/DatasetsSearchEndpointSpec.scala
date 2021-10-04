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

package ch.datascience.knowledgegraph.datasets.rest

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.config.renku
import ch.datascience.config.renku.ResourceUrl
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets._
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.testentities.generators.EntitiesGenerators._
import ch.datascience.http.ErrorMessage
import ch.datascience.http.InfoMessage._
import ch.datascience.http.rest.paging.PagingRequest.Decoders.{page, perPage}
import ch.datascience.http.rest.paging.model.Total
import ch.datascience.http.rest.paging.{PagingHeaders, PagingResponse}
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Warn}
import ch.datascience.knowledgegraph.datasets.model.DatasetCreator
import ch.datascience.knowledgegraph.datasets.rest.DatasetsFinder.{DatasetSearchResult, ProjectsCount}
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.{Phrase, query}
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort
import ch.datascience.logging.TestExecutionTimeRecorder
import eu.timepit.refined.auto._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.http4s.MediaType.application
import org.http4s.Status._
import org.http4s.headers.`Content-Type`
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class DatasetsSearchEndpointSpec
    extends AnyWordSpec
    with MockFactory
    with ScalaCheckPropertyChecks
    with should.Matchers {

  "searchForDatasets" should {

    "respond with OK and the found datasets" in new TestCase {
      forAll(pagingResponses(datasetSearchResultItems)) { pagingResponse =>
        (datasetsFinder.findDatasets _)
          .expects(maybePhrase, sort, pagingRequest, maybeAuthUser)
          .returning(pagingResponse.pure[IO])

        val response = endpoint.searchForDatasets(maybePhrase, sort, pagingRequest, maybeAuthUser).unsafeRunSync()

        response.status       shouldBe Ok
        response.contentType  shouldBe Some(`Content-Type`(application.json))
        response.headers.toList should contain allElementsOf PagingHeaders.from[IO, ResourceUrl](pagingResponse)
        response
          .as[List[Json]]
          .unsafeRunSync() should contain theSameElementsAs (pagingResponse.results map toJson)

        logger.loggedOnly(warn(maybePhrase))
        logger.reset()
      }
    }

    "respond with OK and an empty JSON array when no matching datasets found" in new TestCase {
      val pagingResponse = PagingResponse
        .from[IO, DatasetSearchResult](Nil, pagingRequest, Total(0))
        .unsafeRunSync()
      (datasetsFinder.findDatasets _)
        .expects(maybePhrase, sort, pagingRequest, maybeAuthUser)
        .returning(pagingResponse.pure[IO])

      val response = endpoint.searchForDatasets(maybePhrase, sort, pagingRequest, maybeAuthUser).unsafeRunSync()

      response.status                         shouldBe Ok
      response.contentType                    shouldBe Some(`Content-Type`(application.json))
      response.as[List[Json]].unsafeRunSync() shouldBe empty
      response.headers.toList                   should contain allElementsOf PagingHeaders.from[IO, ResourceUrl](pagingResponse)

      logger.loggedOnly(warn(maybePhrase))
    }

    "respond with INTERNAL_SERVER_ERROR when searching for datasets fails" in new TestCase {
      val exception = exceptions.generateOne
      (datasetsFinder.findDatasets _)
        .expects(maybePhrase, sort, pagingRequest, maybeAuthUser)
        .returning(exception.raiseError[IO, PagingResponse[DatasetSearchResult]])

      val response = endpoint.searchForDatasets(maybePhrase, sort, pagingRequest, maybeAuthUser).unsafeRunSync()

      response.status      shouldBe InternalServerError
      response.contentType shouldBe Some(`Content-Type`(application.json))

      val errorMessage = maybePhrase match {
        case Some(phrase) => s"Finding datasets matching '$phrase' failed"
        case None         => s"Finding all datasets failed"
      }

      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(errorMessage).asJson

      logger.loggedOnly(Error(errorMessage, exception))
    }
  }

  "Sort.properties" should {

    import DatasetsSearchEndpoint.Sort._

    "list only name, datePublished and projectsCount" in {
      DatasetsSearchEndpoint.Sort.properties shouldBe Set(TitleProperty,
                                                          DateProperty,
                                                          DatePublishedProperty,
                                                          ProjectsCountProperty
      )
    }
  }

  private lazy val gitLabUrl = gitLabUrls.generateOne

  private trait TestCase {
    import ch.datascience.json.JsonOps._
    import ch.datascience.tinytypes.json.TinyTypeEncoders._

    val maybePhrase   = phrases.generateOption
    val sort          = searchEndpointSorts.generateOne
    val pagingRequest = pagingRequests.generateOne

    val maybeAuthUser = authUsers.generateOption

    private val renkuResourcesUrl = renkuResourcesUrls.generateOne
    implicit val renkuResourceUrl: renku.ResourceUrl =
      (renkuResourcesUrl / "datasets") ? (page.parameterName -> pagingRequest.page) & (perPage.parameterName -> pagingRequest.perPage) & (Sort.sort.parameterName -> sort) && (query.parameterName -> maybePhrase)

    val datasetsFinder        = mock[DatasetsFinder[IO]]
    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    val endpoint =
      new DatasetsSearchEndpoint[IO](datasetsFinder, renkuResourcesUrl, gitLabUrl, executionTimeRecorder, logger)

    lazy val toJson: DatasetSearchResult => Json = {
      case DatasetSearchResult(id,
                               title,
                               name,
                               maybeDescription,
                               creators,
                               date,
                               exemplarProjectPath,
                               projectsCount,
                               keywords,
                               images
          ) =>
        json"""{
          "identifier": $id,
          "title": $title,
          "name": $name,
          "published": ${creators -> date},
          "date": ${date.instant},
          "projectsCount": ${projectsCount.value},
          "keywords": ${keywords.map(_.value)},
          "images": ${images -> exemplarProjectPath},
          "_links": [{
            "rel": "details",
            "href": ${(renkuResourcesUrl / "datasets" / id).value}
          }]
        }""" addIfDefined "description" -> maybeDescription
    }

    private implicit lazy val publishingEncoder: Encoder[(Set[DatasetCreator], Date)] =
      Encoder.instance[(Set[DatasetCreator], Date)] {
        case (creators, DatePublished(date)) =>
          json"""{
          "creator": $creators,
          "datePublished": $date
        }"""
        case (creators, _) =>
          json"""{
          "creator": $creators
        }"""
      }

    private implicit lazy val creatorEncoder: Encoder[DatasetCreator] = Encoder.instance[DatasetCreator] {
      case DatasetCreator(maybeEmail, name, _) =>
        json"""{
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

    def warn(maybePhrase: Option[Phrase]) = maybePhrase match {
      case Some(phrase) =>
        Warn(s"Finding datasets containing '$phrase' phrase finished${executionTimeRecorder.executionTimeInfo}")
      case None =>
        Warn(s"Finding all datasets finished${executionTimeRecorder.executionTimeInfo}")
    }
  }

  private implicit lazy val datasetSearchResultItems: Gen[DatasetSearchResult] = for {
    id                  <- datasetIdentifiers
    title               <- datasetTitles
    name                <- datasetNames
    maybeDescription    <- Gen.option(datasetDescriptions)
    creators            <- nonEmptySet(personEntities, maxElements = 4)
    dates               <- datasetDates
    exemplarProjectPath <- projectPaths
    projectsCount       <- nonNegativeInts() map (_.value) map ProjectsCount.apply
    keywords            <- listOf(datasetKeywords)
    images              <- listOf(datasetImageUris)
  } yield DatasetSearchResult(id,
                              title,
                              name,
                              maybeDescription,
                              creators.map(_.to[DatasetCreator]),
                              dates,
                              exemplarProjectPath,
                              projectsCount,
                              keywords,
                              images
  )
}
