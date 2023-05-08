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

package io.renku.webhookservice.hookcreation

import cats.effect.IO
import cats.syntax.all._
import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.graph.model.projects.GitLabId
import io.renku.http.ErrorMessage
import io.renku.http.ErrorMessage._
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.http.server.EndpointTester._
import io.renku.http.server.security.model.AuthUser
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.IOSpec
import io.renku.webhookservice.hookcreation.HookCreator.CreationResult.{HookCreated, HookExisted}
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class HookCreationEndpointSpec extends AnyWordSpec with MockFactory with should.Matchers with IOSpec {

  "createHook" should {

    "return CREATED when webhook is successfully created for project with the given id" in new TestCase {

      (hookCreator
        .createHook(_: GitLabId, _: AuthUser))
        .expects(projectId, authUser)
        .returning(HookCreated.some.pure[IO])

      val response = createHook(projectId, authUser).unsafeRunSync()

      response.status                   shouldBe Created
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe json"""{"message": "Hook created"}"""
    }

    "return OK when hook was already created" in new TestCase {

      (hookCreator
        .createHook(_: GitLabId, _: AuthUser))
        .expects(projectId, authUser)
        .returning(HookExisted.some.pure[IO])

      val response = createHook(projectId, authUser).unsafeRunSync()

      response.status                   shouldBe Ok
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe json"""{"message": "Hook already existed"}"""
    }

    "return NOT_FOUND when hook creation returns None" in new TestCase {

      (hookCreator
        .createHook(_: GitLabId, _: AuthUser))
        .expects(projectId, authUser)
        .returning(None.pure[IO])

      val response = createHook(projectId, authUser).unsafeRunSync()

      response.status                   shouldBe NotFound
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe json"""{"message": "Project not found"}"""
    }

    "return INTERNAL_SERVER_ERROR when there was an error during hook creation and log the error" in new TestCase {

      val errorMessage = ErrorMessage("some error")
      val exception    = new Exception(errorMessage.toString())
      (hookCreator
        .createHook(_: GitLabId, _: AuthUser))
        .expects(projectId, authUser)
        .returning(IO.raiseError(exception))

      val response = createHook(projectId, authUser).unsafeRunSync()

      response.status                   shouldBe InternalServerError
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe errorMessage.asJson

      logger.loggedOnly(Error(exception.getMessage, exception))
    }

    "return UNAUTHORIZED when there was an UnauthorizedException thrown during hook creation" in new TestCase {

      (hookCreator
        .createHook(_: GitLabId, _: AuthUser))
        .expects(projectId, authUser)
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

    implicit val logger: TestLogger[IO] = TestLogger[IO]()

    val hookCreator = mock[HookCreator[IO]]
    val createHook  = new HookCreationEndpointImpl[IO](hookCreator).createHook _
  }
}
