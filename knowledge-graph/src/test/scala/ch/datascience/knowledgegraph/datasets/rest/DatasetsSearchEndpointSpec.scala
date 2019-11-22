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

package ch.datascience.knowledgegraph.datasets.rest

import cats.MonadError
import cats.effect.IO
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.InfoMessage._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Warn}
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.model.{DatasetCreator, DatasetPublishing}
import ch.datascience.knowledgegraph.datasets.rest.DatasetsFinder.{DatasetSearchResult, ProjectsCount}
import ch.datascience.logging.TestExecutionTimeRecorder
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.http4s.MediaType.application
import org.http4s.Status._
import org.http4s.headers.`Content-Type`
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class DatasetsSearchEndpointSpec extends WordSpec with MockFactory with ScalaCheckPropertyChecks {

  "searchForDatasets" should {

    "respond with OK and the found datasets" in new TestCase {
      forAll(nonEmptyList(datasetSearchResultItems)) { datasetSearchResults =>
        (datasetsFinder.findDatasets _)
          .expects(phrase, sort)
          .returning(context.pure(datasetSearchResults.toList))

        val response = searchForDatasets(phrase, sort).unsafeRunSync()

        response.status      shouldBe Ok
        response.contentType shouldBe Some(`Content-Type`(application.json))

        response.as[List[Json]].unsafeRunSync should contain theSameElementsAs (datasetSearchResults.toList map toJson)

        logger.loggedOnly(
          Warn(s"Finding datasets containing '$phrase' phrase finished${executionTimeRecorder.executionTimeInfo}")
        )
        logger.reset()
      }
    }

    "respond with OK and an empty JSON array when no matching datasets found" in new TestCase {
      (datasetsFinder.findDatasets _)
        .expects(phrase, sort)
        .returning(context.pure(Nil))

      val response = searchForDatasets(phrase, sort).unsafeRunSync()

      response.status                       shouldBe Ok
      response.contentType                  shouldBe Some(`Content-Type`(application.json))
      response.as[List[Json]].unsafeRunSync shouldBe empty

      logger.loggedOnly(
        Warn(s"Finding datasets containing '$phrase' phrase finished${executionTimeRecorder.executionTimeInfo}")
      )
    }

    "respond with INTERNAL_SERVER_ERROR when searching for datasets fails" in new TestCase {
      val exception = exceptions.generateOne
      (datasetsFinder.findDatasets _)
        .expects(phrase, sort)
        .returning(context.raiseError(exception))

      val response = searchForDatasets(phrase, sort).unsafeRunSync()

      response.status                 shouldBe InternalServerError
      response.contentType            shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync shouldBe ErrorMessage(s"Finding datasets matching $phrase' failed").asJson

      logger.loggedOnly(Error(s"Finding datasets matching $phrase' failed", exception))
    }
  }

  "Sort.properties" should {

    import DatasetsSearchEndpoint.Sort._

    "list only name, datePublished and projectsCount" in {
      DatasetsSearchEndpoint.Sort.properties shouldBe Set(NameProperty, DatePublishedProperty, ProjectsCountProperty)
    }
  }

  private trait TestCase {
    import ch.datascience.json.JsonOps._
    import ch.datascience.tinytypes.json.TinyTypeEncoders._

    val context = MonadError[IO, Throwable]

    val phrase = phrases.generateOne
    val sort   = searchEndpointSorts.generateOne

    val datasetsFinder        = mock[DatasetsFinder[IO]]
    val renkuResourcesUrl     = renkuResourcesUrls.generateOne
    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    val searchForDatasets = new DatasetsSearchEndpoint[IO](
      datasetsFinder,
      renkuResourcesUrl,
      executionTimeRecorder,
      logger
    ).searchForDatasets _

    lazy val toJson: DatasetSearchResult => Json = {
      case DatasetSearchResult(id, name, maybeDescription, published, projectsCount) =>
        json"""{
          "identifier": $id,
          "name": $name,
          "published": $published,
          "projectsCount": ${projectsCount.value},
          "_links": [{
            "rel": "details",
            "href": ${(renkuResourcesUrl / "datasets" / id).value}
          }]
        }""" addIfDefined "description" -> maybeDescription
    }

    private implicit lazy val publishingEncoder: Encoder[DatasetPublishing] = Encoder.instance[DatasetPublishing] {
      case DatasetPublishing(maybeDate, creators) =>
        json"""{
        "creator": $creators
      }""" addIfDefined "datePublished" -> maybeDate
    }

    private implicit lazy val creatorEncoder: Encoder[DatasetCreator] = Encoder.instance[DatasetCreator] {
      case DatasetCreator(maybeEmail, name, _) =>
        json"""{
        "name": $name
      }""" addIfDefined ("email" -> maybeEmail)
    }
  }

  private implicit val datasetSearchResultItems: Gen[DatasetSearchResult] = for {
    id               <- datasetIds
    name             <- datasetNames
    maybeDescription <- Gen.option(datasetDescriptions)
    published        <- datasetPublishingInfos
    projectsCount    <- nonNegativeInts() map (_.value) map ProjectsCount.apply
  } yield DatasetSearchResult(id, name, maybeDescription, published, projectsCount)
}
