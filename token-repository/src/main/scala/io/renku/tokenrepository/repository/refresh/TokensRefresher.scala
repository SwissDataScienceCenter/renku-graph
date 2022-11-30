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

package io.renku.tokenrepository.repository
package refresh

import ProjectsTokensDB.SessionResource
import association._
import cats.effect.{Async, Temporal}
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import deletion.TokenRemover
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.metrics.LabeledHistogram
import io.renku.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import io.renku.tokenrepository.repository.association.TokenStoringInfo.Project
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

trait TokensRefresher[F[_]] {
  def run(): F[Unit]
}

object TokensRefresher {

  import io.renku.config.ConfigLoader._

  import scala.concurrent.duration.FiniteDuration

  def apply[F[_]: Async: SessionResource: GitLabClient: Logger](queriesExecTimes: LabeledHistogram[F],
                                                                config: Config = ConfigFactory.load()
  ): F[TokensRefresher[F]] = for {
    crypto                    <- AccessTokenCrypto[F]()
    tokenValidator            <- TokenValidator[F]
    tokenCreator              <- ProjectAccessTokenCreator[F]()
    expiredTokenCheckInterval <- find[F, FiniteDuration]("expired-project-token-check-interval", config)
  } yield new TokensRefresherImpl[F](
    EventsFinder[F](queriesExecTimes),
    crypto,
    tokenValidator,
    TokenRemover[F](queriesExecTimes),
    tokenCreator,
    AssociationPersister[F](queriesExecTimes),
    expiredTokenCheckInterval
  )
}

private class TokensRefresherImpl[F[_]: Async: Logger](eventsFinder: EventsFinder[F],
                                                       tokenCrypto:          AccessTokenCrypto[F],
                                                       tokenValidator:       TokenValidator[F],
                                                       tokenRemover:         TokenRemover[F],
                                                       tokenCreator:         ProjectAccessTokenCreator[F],
                                                       associationPersister: AssociationPersister[F],
                                                       checkInterval:        FiniteDuration
) extends TokensRefresher[F] {

  private val logPrefix = "project token refresh:"

  import associationPersister._
  import eventsFinder._
  import fs2.Stream
  import tokenCrypto._

  override def run(): F[Unit] =
    Temporal[F].andWait(refreshProcess, checkInterval).foreverM

  private lazy val refreshProcess = Stream
    .repeatEval(findEvent())
    .unNoneTerminate
    .evalMap(decryptToken)
    .evalMap(deleteWhenInvalid)
    .flattenOption
    .evalMap(createNewToken)
    .flattenOption
    .evalMap(encryptNewToken)
    .evalMap(store)
    .evalTap(event => Logger[F].info(show"$logPrefix ${event.project} token recreated"))
    .compile
    .drain
    .recoverWith(logError)

  private def decryptToken(event: TokenCloseExpiration) =
    decrypt(event.encryptedToken)
      .map(event -> _)
      .adaptError(toProcessError(at = "decryption", event.project))

  private lazy val deleteWhenInvalid
      : ((TokenCloseExpiration, AccessToken)) => F[Option[(TokenCloseExpiration, AccessToken)]] = {
    case eventAndToken @ (event, token) =>
      tokenValidator.checkValid(token).adaptError(toProcessError(at = "validation", event.project)) >>= {
        case true => eventAndToken.some.pure[F]
        case false =>
          Logger[F].warn(show"$logPrefix ${event.project} token invalid; deleting") >>
            tokenRemover.delete(event.project.id).adaptError(toProcessError(at = "deletion", event.project)) >>
            Option.empty[(TokenCloseExpiration, AccessToken)].pure[F]
      }
  }

  private lazy val createNewToken
      : ((TokenCloseExpiration, AccessToken)) => F[Option[(TokenCloseExpiration, TokenCreationInfo)]] = {
    case (event, token) =>
      tokenCreator
        .createPersonalAccessToken(event.project.id, token)
        .adaptError(toProcessError(at = "creation", event.project))
        .map(_.map(event -> _))
  }

  private lazy val encryptNewToken: ((TokenCloseExpiration, TokenCreationInfo)) => F[
    (TokenCloseExpiration, TokenCreationInfo, EncryptedAccessToken)
  ] = { case (event, creationInfo) =>
    encrypt(creationInfo.token)
      .adaptError(toProcessError(at = "encryption", event.project))
      .map(enc => (event, creationInfo, enc))
  }

  private lazy val store
      : ((TokenCloseExpiration, TokenCreationInfo, EncryptedAccessToken)) => F[TokenCloseExpiration] = {
    case (event, creationInfo, encToken) =>
      persistAssociation(TokenStoringInfo(event.project, encToken, creationInfo.dates))
        .adaptError(toProcessError(at = "storing", event.project))
        .map(_ => event)
  }

  private def toProcessError(at: String, project: Project): PartialFunction[Throwable, Throwable] = { case ex =>
    ProcessException(project, at, cause = ex)
  }

  private lazy val logError: PartialFunction[Throwable, F[Unit]] = {
    case ex: ProcessException => Logger[F].error(ex.cause)(show"$logPrefix ${ex.getMessage}")
    case ex => Logger[F].error(ex)(show"$logPrefix processing failure")
  }

  private case class ProcessException(project: Project, step: String, cause: Throwable)
      extends Exception(show"failure at '$step' for $project", cause)
}
