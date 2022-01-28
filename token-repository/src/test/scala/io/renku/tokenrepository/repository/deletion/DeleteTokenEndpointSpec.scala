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

package io.renku.tokenrepository.repository.deletion

import cats.effect.IO
import cats.syntax.all._
import io.circe.Json
import io.circe.literal._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects.Id
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.IOSpec
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class DeleteTokenEndpointSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "associateToken" should {

    "respond with NO_CONTENT if the token removal was successful" in new TestCase {

      (tokenRemover
        .delete(_: Id))
        .expects(projectId)
        .returning(IO.unit)

      val response = deleteToken(projectId).unsafeRunSync()

      response.status                                shouldBe Status.NoContent
      response.body.compile.toVector.unsafeRunSync() shouldBe empty

      logger.expectNoLogs()
    }

    "respond with INTERNAL_SERVER_ERROR if token removal fails" in new TestCase {

      val exception = exceptions.generateOne
      (tokenRemover
        .delete(_: Id))
        .expects(projectId)
        .returning(exception.raiseError[IO, Unit])

      val response = deleteToken(projectId).unsafeRunSync()

      response.status      shouldBe Status.InternalServerError
      response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))
      val expectedMessage = s"Deleting token for projectId: $projectId failed"
      response.as[Json].unsafeRunSync() shouldBe json"""{"message": $expectedMessage}"""

      logger.loggedOnly(Error(expectedMessage, exception))
    }
  }

  private trait TestCase {

    val projectId = projectIds.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val tokenRemover = mock[TokenRemover[IO]]
    val deleteToken  = new DeleteTokenEndpointImpl[IO](tokenRemover).deleteToken _
  }
}
