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

package io.renku.webhookservice.hookfetcher

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.literal._
import io.renku.http.client.GitLabGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.http.RenkuEntityCodec
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.rest.paging.model.Page
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.interpreters.TestLogger
import io.renku.testtools.GitLabClientTools
import io.renku.webhookservice.WebhookServiceGenerators.{hookIdAndUrls, projectHookUrls}
import io.renku.webhookservice.hookfetcher.ProjectHookFetcher.HookIdAndUrl
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Request, Response, Status, Uri}
import org.scalacheck.Gen
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.OptionValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class ProjectHookFetcherSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with AsyncMockFactory
    with GitLabClientTools[IO]
    with should.Matchers
    with RenkuEntityCodec
    with OptionValues {

  it should "return list of project hooks from all pages" in {

    val projectId    = projectIds.generateOne
    val uri          = uri"projects" / projectId / "hooks"
    val accessToken  = accessTokens.generateOne
    val resultsPage1 = hookIdAndUrls.toGeneratorOfNonEmptyList(2).generateOne.toList
    val resultsPage2 = hookIdAndUrls.toGeneratorOfNonEmptyList(2).generateOne.toList

    getProjectHooks(uri.withQueryParam("page", 1), accessToken, returning = resultsPage1, maybeNextPage = Page(2).some)
    getProjectHooks(uri.withQueryParam("page", 2), accessToken, returning = resultsPage2, maybeNextPage = None)

    fetcher
      .fetchProjectHooks(projectId, accessToken)
      .asserting(_.value should contain theSameElementsAs (resultsPage1 ::: resultsPage2))
  }

  it should "return the list of hooks if the response is Ok" in {

    val id  = positiveInts().generateOne.value
    val url = projectHookUrls.generateOne
    mapResponse((Status.Ok, Request(), Response().withEntity(json"""[{"id":$id, "url":${url.value}}]""")))
      .asserting(_ shouldBe List(HookIdAndUrl(id, url)).some -> None)
  }

  it should "return an empty list of hooks if the project does not exists" in {
    mapResponse(Status.NotFound, Request(), Response())
      .asserting(_ shouldBe List.empty[HookIdAndUrl].some -> None)
  }

  Status.Unauthorized :: Status.Forbidden :: Nil foreach { status =>
    it should show"return None if remote client responds with $status" in {
      mapResponse(status, Request(), Response())
        .asserting(_ shouldBe None -> None)
    }
  }

  it should "return an Exception if remote client responds with status any of OK , NOT_FOUND, UNAUTHORIZED or FORBIDDEN" in {
    intercept[Exception] {
      mapResponse(Status.ServiceUnavailable, Request(), Response()).assertNoException
    } shouldBe a[MatchError]
  }

  it should "return a RuntimeException if remote client responds with unexpected body" in {
    mapResponse((Status.Ok, Request(), Response().withEntity("""{}""")))
      .assertThrowsError[Exception](_.getMessage should include("Could not decode JSON"))
  }

  private implicit val logger:   TestLogger[IO]   = TestLogger[IO]()
  private implicit val glClient: GitLabClient[IO] = mock[GitLabClient[IO]]
  private lazy val fetcher = new ProjectHookFetcherImpl[IO]

  private def getProjectHooks(uri:           Uri,
                              accessToken:   AccessToken,
                              returning:     List[HookIdAndUrl],
                              maybeNextPage: Option[Page]
  ) = {
    val endpointName: String Refined NonEmpty = "project-hooks"

    (glClient
      .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, (Option[List[HookIdAndUrl]], Option[Page])])(
        _: Option[AccessToken]
      ))
      .expects(uri, endpointName, *, accessToken.some)
      .returning((returning.some -> maybeNextPage).pure[IO])
  }

  private lazy val mapResponse = captureMapping(glClient)(
    fetcher.fetchProjectHooks(projectIds.generateOne, accessTokens.generateOne).unsafeRunSync(),
    Gen.const(Option.empty[List[HookIdAndUrl]] -> Option.empty[Page]),
    underlyingMethod = Get
  )
}
