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

package io.renku.tokenrepository.repository.cleanup

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import fs2.Stream
import io.renku.eventlog
import io.renku.eventlog.api.events.GlobalCommitSyncRequest
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.http.client.GitLabGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.interpreters.TestLogger
import io.renku.tokenrepository.repository.RepositoryGenerators.{deletionResults, encryptedAccessTokens}
import io.renku.tokenrepository.repository.deletion.{DeletionResult, PersistedTokenRemover, TokenRemover}
import org.scalacheck.Gen
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

import scala.concurrent.duration._

class ExpiringTokensRemoverSpec extends AsyncFlatSpec with AsyncIOSpec with should.Matchers with AsyncMockFactory {

  it should "continue fetching tokens that are about to expire and remove them" in {

    val tokens = expiringTokens.generateList(min = 1)
    givenTokensFinding(returning = Stream.emits(tokens))
    tokens foreach { et =>
      givenTokenRemoval(et, returning = deletionResults.generateOne.pure[IO])
    }

    remover.removeExpiringTokens().assertNoException >>
      elClient.waitForArrival(tokens.map(t => GlobalCommitSyncRequest(t.project))).assertNoException
  }

  it should "retry if process fails" in {

    val token1 = expiringTokens.generateOne
    val token2 = expiringTokens.generateOne
    val token3 = expiringTokens.generateOne
    givenTokensFinding(returning = Stream(token1, token2, token3))
    givenTokenRemoval(token1, returning = deletionResults.generateOne.pure[IO])
    val exception = exceptions.generateOne
    givenTokenRemoval(token2, returning = exception.raiseError[IO, Nothing])

    givenTokensFinding(returning = Stream(token2, token3))
    givenTokenRemoval(token2, returning = deletionResults.generateOne.pure[IO])
    givenTokenRemoval(token3, returning = deletionResults.generateOne.pure[IO])

    remover.removeExpiringTokens().assertNoException >>
      elClient
        .waitForArrival(
          List(GlobalCommitSyncRequest(token1.project),
               GlobalCommitSyncRequest(token2.project),
               GlobalCommitSyncRequest(token3.project)
          )
        )
        .assertNoException
  }

  private implicit val logger: TestLogger[IO] = TestLogger()
  private val tokensFinder   = mock[ExpiringTokensFinder[IO]]
  private val tokenRemover   = mock[TokenRemover[IO]]
  private val dbTokenRemover = mock[PersistedTokenRemover[IO]]
  private lazy val elClient  = eventlog.api.events.TestClient.collectingMode[IO]
  private lazy val remover = new ExpiringTokensRemoverImpl[IO](tokensFinder,
                                                               tokenRemover,
                                                               dbTokenRemover,
                                                               elClient,
                                                               retryDelayOnError = 100 millis
  )

  private def givenTokensFinding(returning: Stream[IO, ExpiringToken]) =
    (() => tokensFinder.findExpiringTokens)
      .expects()
      .returning(returning)

  private def givenTokenRemoval(expiringToken: ExpiringToken, returning: IO[DeletionResult]) =
    expiringToken match {
      case ExpiringToken.Decryptable(project, token) =>
        (tokenRemover.delete _)
          .expects(project.id, token.some)
          .returning(returning)
      case ExpiringToken.NonDecryptable(project, _) =>
        (dbTokenRemover.delete _)
          .expects(project.id)
          .returning(returning)
    }

  private lazy val expiringTokens: Gen[ExpiringToken] =
    Gen.oneOf(
      (consumerProjects, accessTokens).mapN(ExpiringToken.Decryptable.apply),
      (consumerProjects, encryptedAccessTokens).mapN(ExpiringToken.NonDecryptable.apply)
    )
}
