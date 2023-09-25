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

package io.renku.webhookservice.hookfetcher

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.literal._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.server.EndpointTester.jsonEntityEncoder
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.{GitLabClientTools, IOSpec}
import io.renku.webhookservice.WebhookServiceGenerators.{hookIdAndUrls, projectHookUrls}
import io.renku.webhookservice.hookfetcher.ProjectHookFetcher.HookIdAndUrl
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Request, Response, Status, Uri}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.OptionValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectHookFetcherSpec
    extends AnyWordSpec
    with MockFactory
    with ExternalServiceStubbing
    with GitLabClientTools[IO]
    with should.Matchers
    with OptionValues
    with IOSpec {

  "fetchProjectHooks" should {

    "return list of project hooks" in new TestCase {

      val idAndUrls = hookIdAndUrls.toGeneratorOfNonEmptyList(2).generateOne.toList

      (gitLabClient
        .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Option[List[HookIdAndUrl]]])(
          _: Option[AccessToken]
        ))
        .expects(uri, endpointName, *, accessToken.some)
        .returning(idAndUrls.some.pure[IO])

      fetcher
        .fetchProjectHooks(projectId, accessToken)
        .unsafeRunSync()
        .value should contain theSameElementsAs idAndUrls
    }

    "return the list of hooks if the response is Ok" in new TestCase {

      val id  = positiveInts().generateOne.value
      val url = projectHookUrls.generateOne
      mapResponse((Status.Ok, Request(), Response().withEntity(json"""[{"id":$id, "url":${url.value}}]""")))
        .unsafeRunSync() shouldBe List(HookIdAndUrl(id, url)).some
    }

    "return an empty list of hooks if the project does not exists" in new TestCase {
      mapResponse(Status.NotFound, Request(), Response()).unsafeRunSync() shouldBe List.empty[HookIdAndUrl].some
    }

    Status.Unauthorized :: Status.Forbidden :: Nil foreach { status =>
      show"return None if remote client responds with $status" in new TestCase {
        mapResponse(status, Request(), Response()).unsafeRunSync() shouldBe None
      }
    }

    "return an Exception if remote client responds with status any of OK , NOT_FOUND, UNAUTHORIZED or FORBIDDEN" in new TestCase {
      intercept[Exception] {
        mapResponse(Status.ServiceUnavailable, Request(), Response()).unsafeRunSync()
      }
    }

    "return a RuntimeException if remote client responds with unexpected body" in new TestCase {
      intercept[RuntimeException] {
        mapResponse((Status.Ok, Request(), Response().withEntity("""{}"""))).unsafeRunSync()
      }.getMessage should include("Could not decode JSON")
    }
  }

  private trait TestCase {
    val projectId = projectIds.generateOne
    val uri       = uri"projects" / projectId / "hooks"
    val endpointName: String Refined NonEmpty = "project-hooks"
    val accessToken = accessTokens.generateOne

    implicit val logger:       TestLogger[IO]   = TestLogger[IO]()
    implicit val gitLabClient: GitLabClient[IO] = mock[GitLabClient[IO]]
    val fetcher = new ProjectHookFetcherImpl[IO]

    val mapResponse = captureMapping(gitLabClient)(fetcher.fetchProjectHooks(projectId, accessToken).unsafeRunSync(),
                                                   Gen.const(Option.empty[List[HookIdAndUrl]]),
                                                   underlyingMethod = Get
    )
  }
}
