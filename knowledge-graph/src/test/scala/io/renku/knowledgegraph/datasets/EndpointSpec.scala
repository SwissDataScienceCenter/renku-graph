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

package io.renku.knowledgegraph.datasets

import DatasetSearchResult._
import Endpoint.Query.{Phrase, query}
import Endpoint.Sort
import cats.effect.IO
import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.config.renku
import io.renku.config.renku.ResourceUrl
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.datasets._
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.{GraphModelGenerators, projects}
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.http.ErrorMessage
import io.renku.http.InfoMessage._
import io.renku.http.rest.paging.PagingRequest.Decoders.{page, perPage}
import io.renku.http.rest.paging.{PagingHeaders, PagingResponse}
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Warn}
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.testtools.IOSpec
import org.http4s.MediaType.application
import org.http4s.Status._
import org.http4s.headers.`Content-Type`
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EndpointSpec extends AnyWordSpec with MockFactory with ScalaCheckPropertyChecks with should.Matchers with IOSpec {

  "searchForDatasets" should {

    "respond with OK and the found datasets" in new TestCase {
      forAll(pagingResponses(datasetSearchResultItems)) { pagingResponse =>
        (datasetsFinder.findDatasets _)
          .expects(maybePhrase, sort, pagingRequest, maybeAuthUser)
          .returning(pagingResponse.pure[IO])

        val response = endpoint.searchForDatasets(maybePhrase, sort, pagingRequest, maybeAuthUser).unsafeRunSync()

        response.status        shouldBe Ok
        response.contentType   shouldBe Some(`Content-Type`(application.json))
        response.headers.headers should contain allElementsOf PagingHeaders.from[ResourceUrl](pagingResponse)
        response
          .as[List[Json]]
          .unsafeRunSync() should contain theSameElementsAs (pagingResponse.results map toJson)

        logger.loggedOnly(warn(maybePhrase))
        logger.reset()
      }
    }

    "respond with OK and an empty JSON array when no matching datasets found" in new TestCase {
      val pagingResponse = PagingResponse.empty[DatasetSearchResult](pagingRequest)
      (datasetsFinder.findDatasets _)
        .expects(maybePhrase, sort, pagingRequest, maybeAuthUser)
        .returning(pagingResponse.pure[IO])

      val response = endpoint.searchForDatasets(maybePhrase, sort, pagingRequest, maybeAuthUser).unsafeRunSync()

      response.status                         shouldBe Ok
      response.contentType                    shouldBe Some(`Content-Type`(application.json))
      response.as[List[Json]].unsafeRunSync() shouldBe empty
      response.headers.headers should contain allElementsOf PagingHeaders.from[ResourceUrl](pagingResponse)

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

    import io.renku.knowledgegraph.datasets.Endpoint.Sort._

    "list only name, datePublished and projectsCount" in {
      Endpoint.Sort.properties shouldBe Set(NameProperty, DateProperty, DatePublishedProperty, ProjectsCountProperty)
    }
  }

  private lazy val gitLabUrl = GraphModelGenerators.gitLabUrls.generateOne

  private trait TestCase {
    import io.renku.json.JsonOps._

    val maybePhrase   = phrases.generateOption
    val sort          = searchEndpointSorts.generateOne
    val pagingRequest = pagingRequests.generateOne

    val maybeAuthUser = authUsers.generateOption

    private val renkuApiUrl = renkuApiUrls.generateOne
    implicit val renkuResourceUrl: renku.ResourceUrl =
      (renkuApiUrl / "datasets") ? (page.parameterName -> pagingRequest.page) & (perPage.parameterName -> pagingRequest.perPage) && (Sort.sort.parameterName -> sort.sortBy.toList) && (query.parameterName -> maybePhrase)

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val datasetsFinder        = mock[DatasetsFinder[IO]]
    val executionTimeRecorder = TestExecutionTimeRecorder[IO]()
    val endpoint              = new EndpointImpl[IO](datasetsFinder, renkuApiUrl, gitLabUrl, executionTimeRecorder)

    lazy val toJson: DatasetSearchResult => Json = {
      case DatasetSearchResult(id,
                               name,
                               slug,
                               maybeDescription,
                               creators,
                               date,
                               exemplarProject,
                               projectsCount,
                               keywords,
                               images
          ) =>
        json"""{
          "identifier":    $id,
          "slug":          $slug,
          "name":          $name,
          "slug":          $name,
          "published":     ${creators -> date},
          "date":          ${date.instant},
          "projectsCount": ${projectsCount.value},
          "keywords":      ${keywords.map(_.value)},
          "images":        ${images -> exemplarProject},
          "_links": [{
            "rel":  "details",
            "href": ${(renkuApiUrl / "datasets" / id).value}
          }]
        }""" addIfDefined "description" -> maybeDescription
    }

    private implicit lazy val publishingEncoder: Encoder[(List[DatasetCreator], CreatedOrPublished)] =
      Encoder.instance[(List[DatasetCreator], CreatedOrPublished)] {
        case (creators, DatePublished(date)) => json"""{
          "creator":       $creators,
          "datePublished": $date
        }"""
        case (creators, _) => json"""{
          "creator": $creators
        }"""
      }

    private implicit lazy val creatorEncoder: Encoder[DatasetCreator] = Encoder.instance[DatasetCreator] {
      case DatasetCreator(maybeEmail, name, _) => json"""{
        "name": $name
      }""" addIfDefined ("email" -> maybeEmail)
    }

    private implicit lazy val imagesEncoder: Encoder[(List[ImageUri], ExemplarProject)] =
      Encoder.instance[(List[ImageUri], ExemplarProject)] { case (images, ExemplarProject(_, path)) =>
        Json.arr(images.map {
          case uri: ImageUri.Relative => json"""{
            "location": $uri,
            "_links": [{
              "rel":  "view",
              "href": ${s"$gitLabUrl/$path/raw/master/$uri"}
            }]
          }"""
          case uri: ImageUri.Absolute => json"""{
            "location": $uri,
            "_links": [{
              "rel":  "view",
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
    id                <- datasetIdentifiers
    slug              <- datasetSlugs
    name              <- datasetNames
    maybeDescription  <- datasetDescriptions.toGeneratorOfOptions
    creators          <- personEntities.toGeneratorOfNonEmptyList(max = 4)
    dates             <- datasetCreatedOrPublished
    exemplarProjectId <- projectResourceIds
    projectsCount     <- nonNegativeInts() map (_.value) map ProjectsCount.apply
    keywords          <- datasetKeywords.toGeneratorOfList()
    images            <- imageUris.toGeneratorOfList()
  } yield DatasetSearchResult(
    id,
    name,
    slug,
    maybeDescription,
    creators.map(_.to[DatasetCreator]).toList,
    dates,
    ExemplarProject(exemplarProjectId, exemplarProjectId.toUnsafe[projects.Path]),
    projectsCount,
    keywords,
    images
  )
}
