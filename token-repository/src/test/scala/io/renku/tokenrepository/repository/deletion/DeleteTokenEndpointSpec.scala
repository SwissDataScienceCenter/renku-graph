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

package io.renku.tokenrepository.repository
package deletion

import RepositoryGenerators.deletionResults
import cats.effect.IO
import cats.syntax.all._
import io.renku.data.Message
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.testtools.CustomAsyncIOSpec
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class DeleteTokenEndpointSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with AsyncMockFactory
    with should.Matchers
    with BeforeAndAfterEach {

  it should "respond with NO_CONTENT if the token removal was successful" in {

    val projectId        = projectIds.generateOne
    val maybeAccessToken = accessTokens.generateOption
    val deletionResult   = deletionResults.generateOne
    (tokenRemover.delete _)
      .expects(projectId, maybeAccessToken)
      .returning(deletionResult.pure[IO])

    endpoint
      .deleteToken(projectId, maybeAccessToken)
      .flatMap { response =>
        response.status.pure[IO].asserting(_ shouldBe Status.NoContent) >>
          response.body.compile.toVector.asserting(_ shouldBe Vector.empty)
      } >> {
      deletionResult match {
        case DeletionResult.Deleted    => IO(logger.loggedOnly(Info(show"Token removed for $projectId")))
        case DeletionResult.NotExisted => IO(logger.expectNoLogs())
      }
    }
  }

  it should "respond with INTERNAL_SERVER_ERROR if token removal fails" in {

    val projectId        = projectIds.generateOne
    val maybeAccessToken = accessTokens.generateOption
    val exception        = exceptions.generateOne
    (tokenRemover.delete _)
      .expects(projectId, maybeAccessToken)
      .returning(exception.raiseError[IO, DeletionResult])

    val expectedMessage = s"Deleting token for projectId: $projectId failed"
    endpoint.deleteToken(projectId, maybeAccessToken).flatMap { response =>
      response.status.pure[IO].asserting(_ shouldBe Status.InternalServerError) >>
        response.contentType.pure[IO].asserting(_ shouldBe Some(`Content-Type`(MediaType.application.json))) >>
        response.as[Message].asserting(_ shouldBe Message.Error.unsafeApply(expectedMessage))
    } >>
      IO(logger.loggedOnly(Error(expectedMessage, exception)))
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private lazy val tokenRemover = mock[TokenRemover[IO]]
  private lazy val endpoint     = new DeleteTokenEndpointImpl[IO](tokenRemover)

  protected override def beforeEach() = logger.reset()
}
