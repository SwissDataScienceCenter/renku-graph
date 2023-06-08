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

package io.renku.knowledgegraph.entities

import cats.effect.IO
import cats.syntax.all._
import io.circe.Json
import io.circe.syntax._
import io.renku.config.renku
import io.renku.config.renku.ResourceUrl
import io.renku.entities.search.Criteria.Filters
import io.renku.entities.search.Generators.modelEntities
import io.renku.entities.search.{Criteria, EntitiesFinder, model}
import io.renku.generators.CommonGraphGenerators.{authUsers, pagingRequests, pagingResponses, sortBys}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators.{gitLabUrls, renkuUrls}
import io.renku.graph.model._
import io.renku.http.ErrorMessage
import io.renku.http.ErrorMessage._
import io.renku.http.rest.paging.{PagingHeaders, PagingResponse}
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.IOSpec
import org.http4s.MediaType.application
import org.http4s.Method.GET
import org.http4s.Status._
import org.http4s.headers.`Content-Type`
import org.http4s.{Request, Uri}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EndpointSpec
    extends AnyWordSpec
    with MockFactory
    with ScalaCheckPropertyChecks
    with should.Matchers
    with IOSpec
    with ModelEncoders {
  private implicit val renkuUrl: RenkuUrl = renkuUrls.generateOne
  private implicit val renkuApiUrl: renku.ApiUrl =
    renku.ApiUrl(s"$renkuUrl/${relativePaths(maxSegments = 1).generateOne}")

  "GET /entities" should {

    "respond with OK and the found entities" in new TestCase {
      forAll(pagingResponses(modelEntities)) { results =>
        (finder.findEntities(_: Criteria)).expects(criteria).returning(results.pure[IO])

        val response = endpoint.`GET /entities`(criteria, request).unsafeRunSync()

        response.status        shouldBe Ok
        response.contentType   shouldBe Some(`Content-Type`(application.json))
        response.headers.headers should contain allElementsOf PagingHeaders.from[ResourceUrl](results)

        response.as[Json].unsafeRunSync() shouldBe results.results.asJson
      }
    }

    "respond with OK using new search when secret header is present" in new TestCase {
      override val finder: EntitiesFinder[IO] = new EntitiesFinder[IO] {
        override def findEntities(criteria: Criteria): IO[PagingResponse[model.Entity]] = ???
      }
      override val finderNew: EntitiesFinder[IO] = mock[EntitiesFinder[IO]]

      forAll(pagingResponses(modelEntities)) { results =>
        (finderNew.findEntities(_: Criteria)).expects(criteria).returning(results.pure[IO])

        val response =
          endpoint.`GET /entities`(criteria, request.putHeaders("Renku-Kg-QueryNew" -> "true")).unsafeRunSync()

        response.status        shouldBe Ok
        response.contentType   shouldBe Some(`Content-Type`(application.json))
        response.headers.headers should contain allElementsOf PagingHeaders.from[ResourceUrl](results)

        response.as[Json].unsafeRunSync() shouldBe results.results.asJson
      }
    }

    "respond with OK with an empty list if no entities found" in new TestCase {
      val results = PagingResponse.empty[model.Entity](pagingRequests.generateOne)
      (finder.findEntities(_: Criteria)).expects(criteria).returning(results.pure[IO])

      val response = endpoint.`GET /entities`(criteria, request).unsafeRunSync()

      response.status        shouldBe Ok
      response.contentType   shouldBe Some(`Content-Type`(application.json))
      response.headers.headers should contain allElementsOf PagingHeaders.from[ResourceUrl](results)

      response.as[Json].unsafeRunSync() shouldBe List.empty[model.Entity].asJson
    }

    "respond with INTERNAL_SERVER_ERROR when finding entities fails" in new TestCase {
      val exception = exceptions.generateOne
      (finder
        .findEntities(_: Criteria))
        .expects(criteria)
        .returning(exception.raiseError[IO, PagingResponse[model.Entity]])

      val response = endpoint.`GET /entities`(criteria, request).unsafeRunSync()

      response.status      shouldBe InternalServerError
      response.contentType shouldBe Some(`Content-Type`(application.json))

      val errorMessage = "Cross-entity search failed"
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage(errorMessage)

      logger.loggedOnly(Error(errorMessage, exception))
    }
  }

  private class TestCase {
    val criteria = criterias.generateOne
    val request  = Request[IO](GET, Uri.fromString(s"/${relativePaths().generateOne}").fold(throw _, identity))

    implicit val renkuResourceUrl: renku.ResourceUrl = renku.ResourceUrl(show"$renkuUrl${request.uri}")
    implicit val logger:           TestLogger[IO]    = TestLogger[IO]()
    implicit val gitLabUrl:        GitLabUrl         = gitLabUrls.generateOne
    val finder = mock[EntitiesFinder[IO]]
    def finderNew = new EntitiesFinder[IO] {
      override def findEntities(criteria: Criteria): IO[PagingResponse[model.Entity]] = ???
    }
    // the endpoint should by default not use the new search graph
    lazy val endpoint = new EndpointImpl[IO](finderNew, finder, renkuUrl, renkuApiUrl, gitLabUrl)
  }

  private lazy val criterias: Gen[Criteria] = for {
    maybeQuery    <- nonEmptyStrings().toGeneratorOf(Filters.Query).toGeneratorOfOptions
    sortingBy     <- sortBys(Criteria.Sort)
    paging        <- pagingRequests
    maybeAuthUser <- authUsers.toGeneratorOfOptions
  } yield Criteria(Filters(maybeQuery), sortingBy, paging, maybeAuthUser)
}
