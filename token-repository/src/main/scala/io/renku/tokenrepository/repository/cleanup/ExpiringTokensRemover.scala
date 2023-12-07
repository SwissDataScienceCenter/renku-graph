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

package io.renku.tokenrepository.repository.cleanup

import cats.effect.{Async, Temporal}
import cats.syntax.all._
import fs2.Stream
import io.renku.eventlog
import io.renku.eventlog.api.events.GlobalCommitSyncRequest
import io.renku.http.client.GitLabClient
import io.renku.metrics.MetricsRegistry
import io.renku.tokenrepository.repository.ProjectsTokensDB.SessionResource
import io.renku.tokenrepository.repository.deletion.{DeletionResult, PersistedTokenRemover, TokenRemover}
import io.renku.tokenrepository.repository.metrics.QueriesExecutionTimes
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

trait ExpiringTokensRemover[F[_]] {
  def removeExpiringTokens(): F[Unit]
}

object ExpiringTokensRemover {
  def apply[F[_]: Async: GitLabClient: SessionResource: Logger: QueriesExecutionTimes: MetricsRegistry]
      : F[ExpiringTokensRemover[F]] =
    (ExpiringTokensFinder[F], TokenRemover[F], eventlog.api.events.Client[F])
      .mapN(new ExpiringTokensRemoverImpl(_, _, PersistedTokenRemover[F], _, 1 minute))
}

private class ExpiringTokensRemoverImpl[F[_]: Async: Logger](tokensFinder: ExpiringTokensFinder[F],
                                                             tokenRemover:      TokenRemover[F],
                                                             dbTokenRemover:    PersistedTokenRemover[F],
                                                             elClient:          eventlog.api.events.Client[F],
                                                             retryDelayOnError: Duration
) extends ExpiringTokensRemover[F] {

  override def removeExpiringTokens(): F[Unit] =
    (Stream.eval(Logger[F].info("Removing expiring tokens")) >> findAndRemoveTokens).compile.toList
      .flatMap(tkns => Logger[F].info(s"Processed ${tkns.length} expired tokens"))
      .handleErrorWith(waitAndRetry)

  private def findAndRemoveTokens: Stream[F, (ExpiringToken, DeletionResult)] =
    tokensFinder.findExpiringTokens
      .evalMap(et => removeToken(et).tupleLeft(et))
      .evalTap { case (et, _) => sendGlobalCommitSyncRequest(et) }
      .evalTap { case (et, result) => Logger[F].info(show"Expiring token for ${et.project.id} $result") }

  private lazy val removeToken: ExpiringToken => F[DeletionResult] = {
    case ExpiringToken.Decryptable(project, token) => tokenRemover.delete(project.id, token.some)
    case ExpiringToken.NonDecryptable(project, _)  => dbTokenRemover.delete(project.id)
  }

  private def sendGlobalCommitSyncRequest(et: ExpiringToken): F[Unit] =
    elClient
      .send(GlobalCommitSyncRequest(et.project))
      .handleErrorWith(
        Logger[F].error(_)(
          show"Failed to send ${GlobalCommitSyncRequest.categoryName} event for ${et.project.id} after expiring token removal"
        )
      )

  private lazy val waitAndRetry: Throwable => F[Unit] =
    Logger[F].error(_)("Expiring tokens clean-up process failed. Retrying") >>
      Temporal[F].delayBy(removeExpiringTokens(), retryDelayOnError)
}
