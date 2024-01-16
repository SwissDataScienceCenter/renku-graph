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

package io.renku.knowledgegraph.entities.currentuser.recentlyviewed

import cats.effect.IO
import cats.syntax.all._
import io.circe.Json
import io.circe.syntax._
import io.renku.config.renku
import io.renku.config.renku.ResourceUrl
import io.renku.data.Message
import io.renku.entities.search.Generators.modelEntities
import io.renku.entities.search.diff.SearchDiffInstances
import io.renku.entities.search.model.Entity
import io.renku.entities.viewings.search.RecentEntitiesFinder
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators.renkuUrls
import io.renku.http.client.GitLabGenerators.gitLabUrls
import io.renku.graph.model.tools.AdditionalMatchers
import io.renku.graph.model.RenkuUrl
import io.renku.http.RenkuEntityCodec
import io.renku.http.client.GitLabUrl
import io.renku.http.rest.paging.model.{Page, PerPage}
import io.renku.http.rest.paging.{PagingHeaders, PagingRequest, PagingResponse}
import io.renku.interpreters.TestLogger
import io.renku.knowledgegraph.entities.ModelEncoders
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.http4s.MediaType.application
import org.http4s.Status._
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EndpointSpec
    extends AnyWordSpec
    with MockFactory
    with ScalaCheckPropertyChecks
    with AdditionalMatchers
    with SearchDiffInstances
    with should.Matchers
    with IOSpec
    with ModelEncoders
    with RenkuEntityCodec {

  implicit val logger:      TestLogger[IO] = TestLogger[IO]()
  implicit val renkuUrl:    RenkuUrl       = renkuUrls.generateOne
  implicit val renkuApiUrl: renku.ApiUrl   = renku.ApiUrl(s"$renkuUrl/${relativePaths(maxSegments = 1).generateOne}")
  implicit val gitLabUrl:   GitLabUrl      = gitLabUrls.generateOne
  implicit val renkuResourceUrl:        renku.ResourceUrl           = renku.ResourceUrl(show"$renkuUrl/some/path")
  implicit val sparqlQueryTimeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()

  val criteria: RecentEntitiesFinder.Criteria = RecentEntitiesFinder.Criteria(Set.empty, authUsers.generateOne, 5)

  def endpoint(finder: RecentEntitiesFinder[IO]): Endpoint[IO] =
    Endpoint.create[IO](renkuApiUrl, gitLabUrl, finder)

  def makeFinderFor(results: IO[List[Entity]]): RecentEntitiesFinder[IO] = {
    val pr = PagingRequest(Page(1), PerPage.default)

    val finder = mock[RecentEntitiesFinder[IO]]
    (finder
      .findRecentlyViewedEntities(_: RecentEntitiesFinder.Criteria))
      .expects(*)
      .returning(results.map(r => PagingResponse(r, pr)))

    finder
  }

  "getRecentlyViewedEntities" should {

    "respond with OK and the found entities" in {
      forAll(Generators.listOf(modelEntities)) { results =>
        val finder = makeFinderFor(IO(results))

        val response = endpoint(finder).getRecentlyViewedEntities(criteria).unsafeRunSync()
        val pageResp = PagingResponse(results, PagingRequest(Page.first, PerPage(5)))
        response.status      shouldBe Ok
        response.contentType shouldBe Some(`Content-Type`(application.json))
        // there is just one page atm
        response.headers.headers should not(contain allElementsOf PagingHeaders.from[ResourceUrl](pageResp))

        response.as[Json].unsafeRunSync() shouldMatchTo results.asJson
      }
    }

    "respond with INTERNAL_SERVER_ERROR when finding entities fails" in {
      val exception = new Exception("boom")
      val finder    = makeFinderFor(IO.raiseError(exception))
      logger.reset()
      val response = endpoint(finder).getRecentlyViewedEntities(criteria).unsafeRunSync()

      response.status      shouldBe InternalServerError
      response.contentType shouldBe Some(`Content-Type`(application.json))

      val errorMessage = "Recent entity search failed!"
      response.as[Message].unsafeRunSync() shouldBe Message.Error.unsafeApply(errorMessage)

      logger.loggedOnly(TestLogger.Level.Error(errorMessage, exception))
    }
  }
}
