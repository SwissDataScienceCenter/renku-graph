/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.projects.datasets

import Generators.projectDatasetGen
import cats.effect.IO
import cats.syntax.all._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.config.renku
import io.renku.config.renku.ResourceUrl
import io.renku.data.Message
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.{GitLabUrl, RenkuUrl}
import io.renku.http.rest.paging.{PagingHeaders, PagingResponse}
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Warn}
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.testtools.CustomAsyncIOSpec
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.http4s.circe.CirceEntityCodec._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EndpointSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with AsyncMockFactory
    with ScalaCheckPropertyChecks
    with should.Matchers
    with BeforeAndAfterEach {

  it should "respond with OK and the found datasets" in {

    val criteria = Endpoint.Criteria(projectSlug)

    val pagingResponse = pagingResponses(projectDatasetGen).generateOne
    givenProjectFinding(criteria, returning = pagingResponse.pure[IO])

    endpoint.`GET /projects/:slug/datasets`(request, criteria) >>= { response =>
      for {
        _ <- response.as[List[Json]].asserting(_ shouldBe pagingResponse.results.map(_.asJson))
        _ = response.status        shouldBe Ok
        _ = response.contentType   shouldBe Some(`Content-Type`(MediaType.application.json))
        _ = response.headers.headers should contain allElementsOf PagingHeaders.from[ResourceUrl](pagingResponse)

        _ = logger.loggedOnly(
              Warn(s"Finding '${criteria.projectSlug}' datasets finished${executionTimeRecorder.executionTimeInfo}")
            )
      } yield ()
    }

  }

  it should "respond with OK an empty JSON array if no datasets found" in {

    val criteria = Endpoint.Criteria(projectSlug)

    givenProjectFinding(criteria, returning = PagingResponse.empty[ProjectDataset](pagingRequests.generateOne).pure[IO])

    endpoint.`GET /projects/:slug/datasets`(request, criteria) >>= { response =>
      for {
        _ <- response.as[List[Json]].asserting(_ shouldBe List.empty)
        _ = response.status shouldBe Ok
        _ =
          logger.loggedOnly(
            Warn(s"Finding '${criteria.projectSlug}' datasets finished${executionTimeRecorder.executionTimeInfo}")
          )
      } yield ()
    }
  }

  it should "respond with INTERNAL_SERVER_ERROR if finding datasets fails" in {

    val criteria = Endpoint.Criteria(projectSlug)

    val exception = exceptions.generateOne
    givenProjectFinding(criteria, returning = exception.raiseError[IO, PagingResponse[ProjectDataset]])

    endpoint.`GET /projects/:slug/datasets`(request, criteria) >>= { response =>
      for {
        _ <- response
               .as[Message]
               .asserting(_ shouldBe Message.Error.unsafeApply(s"Finding ${criteria.projectSlug}'s datasets failed"))
        _ = response.status      shouldBe InternalServerError
        _ = response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))
        _ = logger.loggedOnly(Error(s"Finding ${criteria.projectSlug}'s datasets failed", exception))
      } yield ()
    }
  }

  private lazy val request     = Request[IO]()
  private lazy val projectSlug = projectSlugs.generateOne

  private implicit lazy val encoder: Encoder[ProjectDataset] = ProjectDataset.encoder(projectSlug)

  private lazy val projectDatasetsFinder = mock[ProjectDatasetsFinder[IO]]
  private implicit lazy val renkuApiUrl:      renku.ApiUrl      = renkuApiUrls.generateOne
  private implicit lazy val renkuUrl:         RenkuUrl          = renkuUrls.generateOne
  private implicit lazy val gitLabUrl:        GitLabUrl         = gitLabUrls.generateOne
  private implicit lazy val renkuResourceUrl: renku.ResourceUrl = renku.ResourceUrl(show"$renkuUrl${request.uri}")
  private implicit lazy val logger:           TestLogger[IO]    = TestLogger[IO]()
  private lazy val executionTimeRecorder = TestExecutionTimeRecorder[IO]()
  private lazy val endpoint              = new EndpointImpl[IO](projectDatasetsFinder, executionTimeRecorder)

  protected override def beforeEach() = logger.reset()

  private def givenProjectFinding(criteria: Endpoint.Criteria, returning: IO[PagingResponse[ProjectDataset]]) =
    (projectDatasetsFinder.findProjectDatasets _)
      .expects(criteria)
      .returning(returning)
}
