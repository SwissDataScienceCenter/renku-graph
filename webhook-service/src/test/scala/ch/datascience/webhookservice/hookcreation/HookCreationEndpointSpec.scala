/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.hookcreation

import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators.{authUsers, projectIds}
import ch.datascience.graph.model.projects.Id
import ch.datascience.http.ErrorMessage
import ch.datascience.http.ErrorMessage._
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import ch.datascience.webhookservice.hookcreation.HookCreator.CreationResult.{HookCreated, HookExisted}
import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class HookCreationEndpointSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "createHook" should {

    "return CREATED when webhook is successfully created for project with the given id in in GitLab" in new TestCase {

      (hookCreator
        .createHook(_: Id, _: AccessToken))
        .expects(projectId, authUser.accessToken)
        .returning(IO.pure(HookCreated))

      val response = createHook(projectId, authUser).unsafeRunSync()

      response.status                   shouldBe Created
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe json"""{"message": "Hook created"}"""
    }

    "return OK when hook was already created" in new TestCase {

      (hookCreator
        .createHook(_: Id, _: AccessToken))
        .expects(projectId, authUser.accessToken)
        .returning(IO.pure(HookExisted))

      val response = createHook(projectId, authUser).unsafeRunSync()

      response.status                   shouldBe Ok
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe json"""{"message": "Hook already existed"}"""
    }

    "return INTERNAL_SERVER_ERROR when there was an error during hook creation and log the error" in new TestCase {

      val errorMessage = ErrorMessage("some error")
      val exception    = new Exception(errorMessage.toString())
      (hookCreator
        .createHook(_: Id, _: AccessToken))
        .expects(projectId, authUser.accessToken)
        .returning(IO.raiseError(exception))

      val response = createHook(projectId, authUser).unsafeRunSync()

      response.status                   shouldBe InternalServerError
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe errorMessage.asJson

      logger.loggedOnly(
        Error(exception.getMessage, exception)
      )
    }

    "return UNAUTHORIZED when there was an UnauthorizedException thrown during hook creation" in new TestCase {

      (hookCreator
        .createHook(_: Id, _: AccessToken))
        .expects(projectId, authUser.accessToken)
        .returning(IO.raiseError(UnauthorizedException))

      val response = createHook(projectId, authUser).unsafeRunSync()

      response.status                   shouldBe Unauthorized
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(UnauthorizedException).asJson
    }
  }

  private trait TestCase {

    val projectId = projectIds.generateOne
    val authUser  = authUsers.generateOne

    val logger = TestLogger[IO]()

    val hookCreator = mock[HookCreator[IO]]
    val createHook  = new HookCreationEndpointImpl[IO](hookCreator, logger).createHook _
  }
}
