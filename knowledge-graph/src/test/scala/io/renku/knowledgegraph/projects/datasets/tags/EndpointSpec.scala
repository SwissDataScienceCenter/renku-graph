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

package io.renku.knowledgegraph.projects.datasets.tags

import Endpoint.Criteria
import cats.effect.IO
import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure}
import io.renku.config.renku
import io.renku.config.renku.ResourceUrl
import io.renku.data.Message
import io.renku.generators.CommonGraphGenerators.{authUsers, pagingRequests, pagingResponses}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators.{datasetNames, projectSlugs, renkuUrls}
import io.renku.graph.model.publicationEvents
import io.renku.http.rest.paging.{PagingHeaders, PagingRequest, PagingResponse}
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.IOSpec
import org.http4s.MediaType.application
import org.http4s.Method.GET
import org.http4s.Status.{InternalServerError, Ok}
import org.http4s.circe._
import org.http4s.headers.`Content-Type`
import org.http4s.{EntityDecoder, Request, Uri}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EndpointSpec extends AnyWordSpec with should.Matchers with MockFactory with ScalaCheckPropertyChecks with IOSpec {

  "GET /projects/:slug/datasets/:name/tags" should {

    "respond with OK and the found tags" in new TestCase {
      forAll(pagingResponses(modelTags)) { results =>
        (finder.findTags _).expects(criteria).returning(results.pure[IO])

        val response = endpoint.`GET /projects/:slug/datasets/:name/tags`(criteria).unsafeRunSync()

        response.status        shouldBe Ok
        response.contentType   shouldBe Some(`Content-Type`(application.json))
        response.headers.headers should contain allElementsOf PagingHeaders.from[ResourceUrl](results)
        implicit val decoder: Decoder[model.Tag] = tagsDecoder(results.results)
        response.as[List[model.Tag]].unsafeRunSync() shouldBe results.results
      }
    }

    "respond with OK with an empty list if no tags found" in new TestCase {

      val results = PagingResponse.empty[model.Tag](pagingRequests.generateOne)
      (finder.findTags _).expects(criteria).returning(results.pure[IO])

      val response = endpoint.`GET /projects/:slug/datasets/:name/tags`(criteria).unsafeRunSync()

      response.status        shouldBe Ok
      response.contentType   shouldBe Some(`Content-Type`(application.json))
      response.headers.headers should contain allElementsOf PagingHeaders.from[ResourceUrl](results)
      implicit val decoder: Decoder[model.Tag] = tagsDecoder(results.results)
      response.as[List[model.Tag]].unsafeRunSync() shouldBe Nil
    }

    "respond with INTERNAL_SERVER_ERROR when finding tags fails" in new TestCase {

      val exception = exceptions.generateOne
      (finder.findTags _).expects(criteria).returning(exception.raiseError[IO, PagingResponse[model.Tag]])

      val response = endpoint.`GET /projects/:slug/datasets/:name/tags`(criteria).unsafeRunSync()

      response.status      shouldBe InternalServerError
      response.contentType shouldBe Some(`Content-Type`(application.json))

      val errorMessage = "Project Dataset Tags search failed"
      response.as[Message].unsafeRunSync() shouldBe Message.Error.unsafeApply(errorMessage)

      logger.loggedOnly(Error(errorMessage, exception))
    }
  }

  private lazy val renkuUrl    = renkuUrls.generateOne
  private lazy val renkuApiUrl = renku.ApiUrl(s"$renkuUrl/${relativePaths(maxSegments = 1).generateOne}")

  private trait TestCase {
    val criteria = criterias.generateOne
    implicit val request: Request[IO] =
      Request[IO](GET, Uri.fromString(s"/${relativePaths().generateOne}").fold(throw _, identity))
    implicit val renkuResourceUrl: renku.ResourceUrl = renku.ResourceUrl(show"$renkuUrl${request.uri}")

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val finder   = mock[TagsFinder[IO]]
    val endpoint = new EndpointImpl[IO](finder, renkuUrl, renkuApiUrl)
  }

  private lazy val criterias = for {
    projectSlug <- projectSlugs
    datasetName <- datasetNames
    maybeUser   <- authUsers.toGeneratorOfOptions
  } yield Criteria(projectSlug, datasetName, PagingRequest.default, maybeUser)

  private def tagsDecoder(possibleTags: List[model.Tag]): Decoder[model.Tag] = Decoder.instance { cursor =>
    import io.renku.tinytypes.json.TinyTypeDecoders._
    for {
      name             <- cursor.downField("name").as[publicationEvents.Name]
      startDate        <- cursor.downField("date").as[publicationEvents.StartDate]
      maybeDesc        <- cursor.downField("description").as[Option[publicationEvents.Description]]
      maybeDatasetLink <- cursor._links.map(_.get("dataset-details"))
      tag <- possibleTags
               .find(tag => maybeDatasetLink.exists(link => link.href.show contains tag.datasetId.show))
               .toRight(DecodingFailure(s"No tag for link $maybeDatasetLink", Nil))
    } yield model.Tag(name, startDate, maybeDesc, tag.datasetId)
  }

  private implicit def httpEntityDecoder(implicit decoder: Decoder[model.Tag]): EntityDecoder[IO, List[model.Tag]] =
    jsonOf[IO, List[model.Tag]]
}
