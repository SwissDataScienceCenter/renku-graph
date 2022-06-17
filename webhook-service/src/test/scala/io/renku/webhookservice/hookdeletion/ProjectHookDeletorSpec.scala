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

package io.renku.webhookservice.hookdeletion

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import eu.timepit.refined.types.all.NonEmptyString
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits.GenOps
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.{GitLabClientTools, IOSpec}
import io.renku.webhookservice.WebhookServiceGenerators.{hookIdAndUrls, projectHookUrls, serializedHookTokens}
import io.renku.webhookservice.hookdeletion.HookDeletor.DeletionResult
import io.renku.webhookservice.hookdeletion.ProjectHookDeletor.ProjectHook
import org.http4s.Method.DELETE
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Request, Response, Status, Uri}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectHookDeletorSpec
    extends AnyWordSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers
    with GitLabClientTools[IO]
    with IOSpec {

  "delete" should {

    "return the DeletionResult from GitLabClient" in new TestCase {
      val accessToken = accessTokens.generateOne

      val result = deletionResults.generateOne

      (gitLabClient
        .delete(_: Uri, _: NonEmptyString)(_: ResponseMappingF[IO, DeletionResult])(_: Option[AccessToken]))
        .expects(uri, endpointName, *, accessToken.some)
        .returning(result.pure[IO])

      hookDeletor
        .delete(projectId, hookIdAndUrl, accessToken)
        .unsafeRunSync() shouldBe result
    }

    // mapResponse

    "return DeletionResult.HookDeleted when response is Ok" in new TestCase {
      mapResponse(Status.Ok, Request(), Response()).unsafeRunSync() shouldBe DeletionResult.HookDeleted
    }

    "return DeletionResult.NotFound when response is NotFound" in new TestCase {
      mapResponse(Status.NotFound, Request(), Response()).unsafeRunSync() shouldBe DeletionResult.HookNotFound
    }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {
      intercept[UnauthorizedException] {
        mapResponse(Status.Unauthorized, Request(), Response()).unsafeRunSync()
      }
    }

    "return an Exception if remote client responds with status neither OK, NOT_FOUND or UNAUTHORIZED" in new TestCase {
      intercept[Exception] {
        mapResponse(Status.ServiceUnavailable, Request(), Response()).unsafeRunSync()
      }
    }
  }

  private trait TestCase {
    val hookIdAndUrl = hookIdAndUrls.generateOne
    val projectId    = projectIds.generateOne
    val uri          = uri"projects" / projectId.show / "hooks" / hookIdAndUrl.id.show
    val endpointName: NonEmptyString = "delete-hook"

    implicit val logger:       TestLogger[IO]   = TestLogger[IO]()
    implicit val gitLabClient: GitLabClient[IO] = mock[GitLabClient[IO]]
    val hookDeletor = new ProjectHookDeletorImpl[IO]

    lazy val mapResponse =
      captureMapping(hookDeletor, gitLabClient)(
        _.delete(projectIds.generateOne, hookIdAndUrls.generateOne, accessTokens.generateOne).unsafeRunSync(),
        Gen.const(DeletionResult.HookDeleted),
        method = DELETE
      )

    val deletionResults: Gen[DeletionResult] = Gen.oneOf(DeletionResult.HookDeleted, DeletionResult.HookNotFound)
  }

  private implicit lazy val projectHooks: Gen[ProjectHook] = for {
    projectId           <- projectIds
    hookUrl             <- projectHookUrls
    serializedHookToken <- serializedHookTokens
  } yield ProjectHook(projectId, hookUrl, serializedHookToken)
}
