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

package io.renku.knowledgegraph.users.projects

import cats.effect.IO
import cats.syntax.all._
import finder.ProjectsFinder
import io.circe.{Decoder, DecodingFailure}
import io.renku.config.renku
import io.renku.config.renku.ResourceUrl
import io.renku.generators.CommonGraphGenerators.{pagingRequests, pagingResponses}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, relativePaths}
import io.renku.graph.model.GraphModelGenerators.renkuUrls
import io.renku.graph.model.{persons, projects}
import io.renku.http.ErrorMessage
import io.renku.http.ErrorMessage._
import io.renku.http.rest.Links
import io.renku.http.rest.paging.{PagingHeaders, PagingResponse}
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.knowledgegraph.projectdetails
import io.renku.testtools.IOSpec
import org.http4s.MediaType.application
import org.http4s.Method.GET
import org.http4s.Status.{InternalServerError, Ok}
import org.http4s.circe.jsonOf
import org.http4s.headers.`Content-Type`
import org.http4s.{EntityDecoder, Request, Uri}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EndpointSpec extends AnyWordSpec with should.Matchers with IOSpec with MockFactory with ScalaCheckPropertyChecks {

  "GET /users/:id/projects" should {

    "return OK with the found projects" in new TestCase {
      forAll(pagingResponses(modelProjects)) { results =>
        (projectsFinder.findProjects _).expects(criteria).returning(results.pure[IO])

        val response = endpoint.`GET /users/:id/projects`(criteria, request).unsafeRunSync()

        response.status        shouldBe Ok
        response.contentType   shouldBe Some(`Content-Type`(application.json))
        response.headers.headers should contain allElementsOf PagingHeaders.from[ResourceUrl](results)
        response.as[List[model.Project]].unsafeRunSync() shouldBe results.results.map(sortKeywords)
      }
    }

    "respond with OK with an empty list if no projects found" in new TestCase {
      val results = PagingResponse.empty[model.Project](pagingRequests.generateOne)
      (projectsFinder.findProjects _).expects(criteria).returning(results.pure[IO])

      val response = endpoint.`GET /users/:id/projects`(criteria, request).unsafeRunSync()

      response.status        shouldBe Ok
      response.contentType   shouldBe Some(`Content-Type`(application.json))
      response.headers.headers should contain allElementsOf PagingHeaders.from[ResourceUrl](results)
      response.as[List[model.Project]].unsafeRunSync() shouldBe Nil
    }

    "respond with INTERNAL_SERVER_ERROR when finding entities fails" in new TestCase {
      val exception = exceptions.generateOne
      (projectsFinder.findProjects _)
        .expects(criteria)
        .returning(exception.raiseError[IO, PagingResponse[model.Project]])

      val response = endpoint.`GET /users/:id/projects`(criteria, request).unsafeRunSync()

      response.status      shouldBe InternalServerError
      response.contentType shouldBe Some(`Content-Type`(application.json))

      val errorMessage = "Finding user's projects failed"
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage(errorMessage)

      logger.loggedOnly(Error(errorMessage, exception))
    }
  }

  private lazy val renkuUrl    = renkuUrls.generateOne
  private lazy val renkuApiUrl = renku.ApiUrl(s"$renkuUrl/${relativePaths(maxSegments = 1).generateOne}")

  private trait TestCase {
    val criteria = criterias.generateOne

    val request = Request[IO](GET, Uri.fromString(s"/${relativePaths().generateOne}").fold(throw _, identity))
    implicit val renkuResourceUrl: renku.ResourceUrl = renku.ResourceUrl(show"$renkuUrl${request.uri}")

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val projectsFinder = mock[ProjectsFinder[IO]]
    val endpoint       = new EndpointImpl[IO](projectsFinder, renkuUrl, renkuApiUrl)
  }

  private implicit def httpEntityDecoder(implicit
      projectDecoder: Decoder[model.Project]
  ): EntityDecoder[IO, List[model.Project]] = jsonOf[IO, List[model.Project]]

  private implicit val projectDecoder: Decoder[model.Project] = cursor => {
    import io.renku.tinytypes.json.TinyTypeDecoders._

    cursor._links >>= {
      case links if links.get(Links.Rel("details")).isDefined =>
        for {
          path         <- cursor.downField("path").as[projects.Path]
          name         <- cursor.downField("name").as[projects.Name]
          visibility   <- cursor.downField("visibility").as[projects.Visibility]
          date         <- cursor.downField("date").as[projects.DateCreated]
          maybeCreator <- cursor.downField("creator").as[Option[persons.Name]]
          keywords     <- cursor.downField("keywords").as[List[projects.Keyword]]
          maybeDesc    <- cursor.downField("description").as[Option[projects.Description]]
          _ <- links
                 .get(Links.Rel("details"))
                 .toRight(DecodingFailure("No 'details' link", Nil))
                 .flatMap { link =>
                   val expected = projectdetails.Endpoint.href(renkuApiUrl, path)
                   Either.cond(link.href.value == expected.show, (), DecodingFailure(s"$link not equal $expected", Nil))
                 }
        } yield model.Project.Activated(name, path, visibility, date, maybeCreator, keywords, maybeDesc)
      case links if links.get(Links.Rel("activation")).isDefined =>
        for {
          id           <- cursor.downField("id").as[projects.Id]
          path         <- cursor.downField("path").as[projects.Path]
          name         <- cursor.downField("name").as[projects.Name]
          visibility   <- cursor.downField("visibility").as[projects.Visibility]
          date         <- cursor.downField("date").as[projects.DateCreated]
          maybeCreator <- cursor.downField("creator").as[Option[persons.Name]]
          keywords     <- cursor.downField("keywords").as[List[projects.Keyword]]
          maybeDesc    <- cursor.downField("description").as[Option[projects.Description]]
          _ <- links
                 .get(Links.Rel("activation"))
                 .toRight(DecodingFailure("No 'activation' link", Nil))
                 .flatMap { link =>
                   val expected = renkuUrl / "api" / "projects" / id.show / "graph" / "webhooks"
                   Either.cond(link.href.value == expected.show,
                               (),
                               DecodingFailure(s"$link not equal $expected", Nil)
                   ) >> Either.cond(link.href.value == expected.show,
                                    (),
                                    DecodingFailure(s"$link not equal $expected", Nil)
                   )
                 }
        } yield model.Project.NotActivated(id, name, path, visibility, date, maybeCreator, keywords, maybeDesc)
      case _ => fail("Neither 'details' nor 'activation' link in the response")
    }
  }

  private lazy val sortKeywords: model.Project => model.Project = {
    case p: model.Project.Activated    => p.copy(keywords = p.keywords.sorted)
    case p: model.Project.NotActivated => p.copy(keywords = p.keywords.sorted)
  }
}
