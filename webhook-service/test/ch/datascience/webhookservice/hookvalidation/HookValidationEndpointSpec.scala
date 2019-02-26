/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.hookvalidation

import cats.MonadError
import cats.effect.IO
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.events.EventsGenerators.projectIds
import ch.datascience.graph.model.events.ProjectId
import ch.datascience.http.client.AccessToken
import ch.datascience.http.server.EndpointTester._
import ch.datascience.webhookservice.exceptions.UnauthorizedException
import ch.datascience.webhookservice.hookvalidation.HookValidator.HookValidationResult._
import ch.datascience.webhookservice.security.IOAccessTokenFinder
import io.circe.Json
import io.circe.syntax._
import org.http4s.Status._
import org.http4s.{Method, Request, Uri}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class HookValidationEndpointSpec extends WordSpec with MockFactory {

  "POST /projects/:id/webhooks/validation" should {

    "return OK when a valid access token is present in the header " +
      "and the hook exists for the project with the given id" in new TestCase {

      val accessToken = accessTokens.generateOne
      (accessTokenFinder
        .findAccessToken(_: Request[IO]))
        .expects(*)
        .returning(context.pure(accessToken))

      (hookValidator
        .validateHook(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.pure(HookExists))

      val response = endpoint.call(
        Request(Method.POST, Uri.uri("projects") / projectId.toString / "webhooks" / "validation")
      )

      response.status       shouldBe Ok
      response.body[String] shouldBe ""
    }

    "return NOT_FOUND the hook does not exist" in new TestCase {

      val accessToken = accessTokens.generateOne
      (accessTokenFinder
        .findAccessToken(_: Request[IO]))
        .expects(*)
        .returning(context.pure(accessToken))

      (hookValidator
        .validateHook(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(context.pure(HookMissing))

      val response = endpoint.call(
        Request(Method.POST, Uri.uri("projects") / projectId.toString / "webhooks" / "validation")
      )

      response.status       shouldBe NotFound
      response.body[String] shouldBe ""
    }

    "return UNAUTHORIZED when finding an access token in the headers fails with UnauthorizedException" in new TestCase {

      (accessTokenFinder
        .findAccessToken(_: Request[IO]))
        .expects(*)
        .returning(context.raiseError(UnauthorizedException))

      val response = endpoint.call(
        Request(Method.POST, Uri.uri("projects") / projectId.toString / "webhooks" / "validation")
      )

      response.status     shouldBe Unauthorized
      response.body[Json] shouldBe ErrorMessage(UnauthorizedException.getMessage).asJson
    }

    "return INTERNAL_SERVER_ERROR when there was an error during hook validation" in new TestCase {
      val accessToken = accessTokens.generateOne
      (accessTokenFinder
        .findAccessToken(_: Request[IO]))
        .expects(*)
        .returning(context.pure(accessToken))

      val errorMessage = ErrorMessage("some error")
      (hookValidator
        .validateHook(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(IO.raiseError(new Exception(errorMessage.toString())))

      val response = endpoint.call(
        Request(Method.POST, Uri.uri("projects") / projectId.toString / "webhooks" / "validation")
      )

      response.status     shouldBe InternalServerError
      response.body[Json] shouldBe errorMessage.asJson
    }

    "return UNAUTHORIZED when there was an UnauthorizedException thrown during hook validation" in new TestCase {

      val accessToken = accessTokens.generateOne
      (accessTokenFinder
        .findAccessToken(_: Request[IO]))
        .expects(*)
        .returning(context.pure(accessToken))

      (hookValidator
        .validateHook(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(IO.raiseError(UnauthorizedException))

      val response = endpoint.call(
        Request(Method.POST, Uri.uri("projects") / projectId.toString / "webhooks" / "validation")
      )

      response.status     shouldBe Unauthorized
      response.body[Json] shouldBe ErrorMessage(UnauthorizedException.getMessage).asJson
    }
  }

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    val projectId = projectIds.generateOne

    val hookValidator     = mock[IOHookValidator]
    val accessTokenFinder = mock[IOAccessTokenFinder]
    val endpoint = new HookValidationEndpoint[IO](
      hookValidator,
      accessTokenFinder
    ).validateHook.or(notAvailableResponse)
  }
}
