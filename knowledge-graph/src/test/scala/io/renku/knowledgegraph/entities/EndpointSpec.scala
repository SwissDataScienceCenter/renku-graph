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

package io.renku.knowledgegraph.entities

import Endpoint._
import cats.effect.IO
import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure}
import io.renku.config.renku
import io.renku.config.renku.ResourceUrl
import io.renku.generators.CommonGraphGenerators.{authUsers, pagingRequests, pagingResponses, renkuResourcesUrls, sortBys}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model._
import io.renku.http.ErrorMessage
import io.renku.http.ErrorMessage._
import io.renku.http.rest.Links
import io.renku.http.rest.paging.{PagingHeaders, PagingResponse}
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.knowledgegraph.entities.Endpoint.Criteria.Filters
import io.renku.knowledgegraph.entities.model.MatchingScore
import io.renku.testtools.IOSpec
import org.http4s.MediaType.application
import org.http4s.Method.GET
import org.http4s.Status._
import org.http4s.circe.jsonOf
import org.http4s.headers.`Content-Type`
import org.http4s.{EntityDecoder, Request, Uri}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EndpointSpec extends AnyWordSpec with MockFactory with ScalaCheckPropertyChecks with should.Matchers with IOSpec {

  "GET /entities" should {

    "respond with OK and the found entities" in new TestCase {
      val results = pagingResponses(modelEntities).generateOne
      (finder.findEntities _).expects(criteria).returning(results.pure[IO])

      val response = endpoint.`GET /entities`(criteria, request).unsafeRunSync()

      response.status        shouldBe Ok
      response.contentType   shouldBe Some(`Content-Type`(application.json))
      response.headers.headers should contain allElementsOf PagingHeaders.from[IO, ResourceUrl](results)
      response.as[List[model.Entity]].unsafeRunSync() shouldBe results.results
    }

    "respond with OK with an empty list if no entities found" in new TestCase {
      val results = PagingResponse.empty[model.Entity](pagingRequests.generateOne)
      (finder.findEntities _).expects(criteria).returning(results.pure[IO])

      val response = endpoint.`GET /entities`(criteria, request).unsafeRunSync()

      response.status        shouldBe Ok
      response.contentType   shouldBe Some(`Content-Type`(application.json))
      response.headers.headers should contain allElementsOf PagingHeaders.from[IO, ResourceUrl](results)
      response.as[List[model.Entity]].unsafeRunSync() shouldBe Nil
    }

    "respond with INTERNAL_SERVER_ERROR when finding entities fails" in new TestCase {
      val exception = exceptions.generateOne
      (finder.findEntities _).expects(criteria).returning(exception.raiseError[IO, PagingResponse[model.Entity]])

      val response = endpoint.`GET /entities`(criteria, request).unsafeRunSync()

      response.status      shouldBe InternalServerError
      response.contentType shouldBe Some(`Content-Type`(application.json))

      val errorMessage = "Cross-entity search failed"
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage(errorMessage)

      logger.loggedOnly(Error(errorMessage, exception))
    }
  }

  "EntityType" should {

    Endpoint.Criteria.Filters.EntityType.all.map {
      case t @ Endpoint.Criteria.Filters.EntityType.Project  => "project"  -> t
      case t @ Endpoint.Criteria.Filters.EntityType.Dataset  => "dataset"  -> t
      case t @ Endpoint.Criteria.Filters.EntityType.Workflow => "workflow" -> t
      case t @ Endpoint.Criteria.Filters.EntityType.Person   => "person"   -> t
    } foreach { case (name, t) =>
      s"be instantiatable from '$name'" in {
        Endpoint.Criteria.Filters.EntityType.from(name) shouldBe t.asRight
      }
    }

  }

  private lazy val renkuResourcesUrl = renkuResourcesUrls.generateOne

  private trait TestCase {
    val criteria = criterias.generateOne
    val request  = Request[IO](GET, Uri.fromString(relativePaths().generateOne).fold(throw _, identity))

    implicit val renkuResourceUrl: renku.ResourceUrl = renkuResourcesUrl / request.uri.show

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val finder   = mock[EntitiesFinder[IO]]
    val endpoint = new EndpointImpl[IO](finder, renkuResourcesUrl)
  }

  private lazy val criterias: Gen[Criteria] = for {
    maybeQuery    <- nonEmptyStrings().toGeneratorOf(Filters.Query).toGeneratorOfOptions
    sortingBy     <- sortBys(Endpoint.Criteria.Sorting)
    paging        <- pagingRequests
    maybeAuthUser <- authUsers.toGeneratorOfOptions
  } yield Criteria(Filters(maybeQuery), sortingBy, paging, maybeAuthUser)

  private implicit lazy val httpEntityDecoder: EntityDecoder[IO, List[model.Entity]] = jsonOf[IO, List[model.Entity]]

  private implicit lazy val entitiesDecoder: Decoder[model.Entity] = cursor => {
    import io.renku.tinytypes.json.TinyTypeDecoders._

    cursor.downField("type").as[Endpoint.Criteria.Filters.EntityType] >>= {
      case Endpoint.Criteria.Filters.EntityType.Project =>
        for {
          score        <- cursor.downField("matchingScore").as[MatchingScore]
          name         <- cursor.downField("name").as[projects.Name]
          path         <- cursor.downField("path").as[projects.Path]
          namespace    <- cursor.downField("namespace").as[String]
          visibility   <- cursor.downField("visibility").as[projects.Visibility]
          date         <- cursor.downField("date").as[projects.DateCreated]
          maybeCreator <- cursor.downField("creator").as[Option[persons.Name]]
          keywords     <- cursor.downField("keywords").as[List[projects.Keyword]]
          maybeDesc    <- cursor.downField("description").as[Option[projects.Description]]
          _ <- Either.cond(path.value startsWith namespace,
                           (),
                           DecodingFailure(s"'$path' does not start with '$namespace'", Nil)
               )
          _ <- cursor._links
                 .flatMap(_.get(Links.Rel("details")).toRight(DecodingFailure("No project details link", Nil)))
                 .flatMap { link =>
                   val expected = renkuResourcesUrl / "projects" / path
                   Either.cond(link.href.value == expected.show, (), DecodingFailure(s"$link not equal $expected", Nil))
                 }
        } yield model.Entity.Project(score, path, name, visibility, date, maybeCreator, keywords, maybeDesc)
      case Endpoint.Criteria.Filters.EntityType.Dataset =>
        for {
          score      <- cursor.downField("matchingScore").as[MatchingScore]
          name       <- cursor.downField("name").as[datasets.Name]
          visibility <- cursor.downField("visibility").as[projects.Visibility]
          date <- cursor
                    .downField("date")
                    .as[datasets.DateCreated]
                    .orElse(cursor.downField("date").as[datasets.DatePublished])
          creators  <- cursor.downField("creators").as[List[persons.Name]]
          keywords  <- cursor.downField("keywords").as[List[datasets.Keyword]]
          maybeDesc <- cursor.downField("description").as[Option[datasets.Description]]
          dsDetailsLink <-
            cursor._links.flatMap(_.get(Links.Rel("details")).toRight(DecodingFailure("No dataset details link", Nil)))
          identifier <- dsDetailsLink.href.value
                          .split("/")
                          .lastOption
                          .toRight(DecodingFailure("Invalid dataset-details link", Nil))
                          .map(datasets.Identifier)
          _ <- {
            val expected = renkuResourcesUrl / "datasets" / identifier
            Either.cond(dsDetailsLink.href.value == expected.show,
                        (),
                        DecodingFailure(s"$dsDetailsLink not equal $expected", Nil)
            )
          }
        } yield model.Entity.Dataset(score, identifier, name, visibility, date, creators, keywords, maybeDesc)
      case Endpoint.Criteria.Filters.EntityType.Workflow =>
        for {
          score      <- cursor.downField("matchingScore").as[MatchingScore]
          name       <- cursor.downField("name").as[plans.Name]
          visibility <- cursor.downField("visibility").as[projects.Visibility]
          date       <- cursor.downField("date").as[plans.DateCreated]
          keywords   <- cursor.downField("keywords").as[List[plans.Keyword]]
          maybeDesc  <- cursor.downField("description").as[Option[plans.Description]]
        } yield model.Entity.Workflow(score, name, visibility, date, keywords, maybeDesc)
      case Endpoint.Criteria.Filters.EntityType.Person =>
        for {
          score <- cursor.downField("matchingScore").as[MatchingScore]
          name  <- cursor.downField("name").as[persons.Name]
        } yield model.Entity.Person(score, name)
    }
  }
}
