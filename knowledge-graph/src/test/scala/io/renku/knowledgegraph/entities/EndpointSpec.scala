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
import io.circe.Decoder._
import io.circe.{Decoder, DecodingFailure}
import io.renku.config.renku
import io.renku.config.renku.ResourceUrl
import io.renku.generators.CommonGraphGenerators.{authUsers, pagingRequests, pagingResponses, sortBys}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators.{gitLabUrls, renkuUrls}
import io.renku.graph.model._
import io.renku.http.ErrorMessage
import io.renku.http.ErrorMessage._
import io.renku.http.rest.Links
import io.renku.http.rest.Links.Rel
import io.renku.http.rest.paging.{PagingHeaders, PagingResponse}
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.knowledgegraph.entities.Endpoint.Criteria.Filters
import io.renku.knowledgegraph.entities.finder.EntitiesFinder
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
      forAll(pagingResponses(modelEntities)) { results =>
        (finder.findEntities _).expects(criteria).returning(results.pure[IO])

        val response = endpoint.`GET /entities`(criteria, request).unsafeRunSync()

        response.status        shouldBe Ok
        response.contentType   shouldBe Some(`Content-Type`(application.json))
        response.headers.headers should contain allElementsOf PagingHeaders.from[IO, ResourceUrl](results)
        implicit val decoder: Decoder[model.Entity] = entitiesDecoder(possibleEntities = results.results)
        response.as[List[model.Entity]].unsafeRunSync() shouldBe results.results
      }
    }

    "respond with OK with an empty list if no entities found" in new TestCase {
      val results = PagingResponse.empty[model.Entity](pagingRequests.generateOne)
      (finder.findEntities _).expects(criteria).returning(results.pure[IO])

      val response = endpoint.`GET /entities`(criteria, request).unsafeRunSync()

      response.status        shouldBe Ok
      response.contentType   shouldBe Some(`Content-Type`(application.json))
      response.headers.headers should contain allElementsOf PagingHeaders.from[IO, ResourceUrl](results)
      implicit val decoder: Decoder[model.Entity] = entitiesDecoder(possibleEntities = results.results)
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

  private lazy val renkuUrl = renkuUrls.generateOne
  private lazy val renkuResourcesUrl =
    renku.ResourcesUrl(s"$renkuUrl/${relativePaths(maxSegments = 1).generateOne}")

  private trait TestCase {
    val criteria = criterias.generateOne
    val request  = Request[IO](GET, Uri.fromString(s"/${relativePaths().generateOne}").fold(throw _, identity))

    implicit val renkuResourceUrl: renku.ResourceUrl = renku.ResourceUrl(show"$renkuUrl${request.uri}")
    implicit val logger:           TestLogger[IO]    = TestLogger[IO]()
    implicit val gitLabUrl:        GitLabUrl         = gitLabUrls.generateOne
    val finder   = mock[EntitiesFinder[IO]]
    val endpoint = new EndpointImpl[IO](finder, renkuUrl, renkuResourcesUrl, gitLabUrl)
  }

  private lazy val criterias: Gen[Criteria] = for {
    maybeQuery    <- nonEmptyStrings().toGeneratorOf(Filters.Query).toGeneratorOfOptions
    sortingBy     <- sortBys(Endpoint.Criteria.Sorting)
    paging        <- pagingRequests
    maybeAuthUser <- authUsers.toGeneratorOfOptions
  } yield Criteria(Filters(maybeQuery), sortingBy, paging, maybeAuthUser)

  private implicit def httpEntityDecoder(implicit
      decoder: Decoder[model.Entity]
  ): EntityDecoder[IO, List[model.Entity]] = jsonOf[IO, List[model.Entity]]

  private def entitiesDecoder(
      possibleEntities: List[model.Entity]
  )(implicit gitLabUrl: GitLabUrl): Decoder[model.Entity] = cursor => {
    import io.renku.tinytypes.json.TinyTypeDecoders._

    def imagesDecoder(projectPath: projects.Path): Decoder[datasets.ImageUri] = Decoder.instance { cursor =>
      cursor.downField("location").as[datasets.ImageUri] >>= {
        case uri: datasets.ImageUri.Relative =>
          cursor._links >>= {
            _.get(Rel("view")) match {
              case Some(link) if link.href.value == s"$gitLabUrl/$projectPath/raw/master/$uri" => uri.asRight
              case maybeLink => DecodingFailure(s"'$maybeLink' is not expected absolute DS image view link", Nil).asLeft
            }
          }
        case uri: datasets.ImageUri.Absolute =>
          cursor._links >>= {
            _.get(Rel("view")) match {
              case Some(link) if link.href.value == uri.show => uri.asRight
              case maybeLink => DecodingFailure(s"'$maybeLink' is not expected relative DS image view link", Nil).asLeft
            }
          }
      }
    }

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
          projectPath <- possibleEntities.collect {
                           case ds: model.Entity.Dataset if ds.identifier == identifier => ds.exemplarProjectPath
                         } match {
                           case path :: Nil => path.asRight
                           case _ => DecodingFailure(s"DS $identifier doesn't exist in possible results", Nil).asLeft
                         }
          images <- cursor.downField("images").as(decodeList(imagesDecoder(projectPath)))
          _ <- {
            val expected = renkuResourcesUrl / "datasets" / identifier
            Either.cond(dsDetailsLink.href.value == expected.show,
                        (),
                        DecodingFailure(s"$dsDetailsLink not equal $expected", Nil)
            )
          }
        } yield model.Entity.Dataset(score,
                                     identifier,
                                     name,
                                     visibility,
                                     date,
                                     creators,
                                     keywords,
                                     maybeDesc,
                                     images,
                                     projectPath
        )
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
