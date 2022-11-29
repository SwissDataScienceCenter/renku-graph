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
package init

import AccessTokenCrypto.EncryptedAccessToken
import ProjectsTokensDB.SessionResource
import association.TokenStoringInfo.Project
import association._
import cats.effect.{Async, Temporal}
import cats.syntax.all._
import deletion.TokenRemover
import io.renku.db.DbClient
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.metrics.LabeledHistogram
import org.typelevel.log4cats.Logger
import skunk.~

import scala.concurrent.duration._

private object TokensMigrator {
  def apply[F[_]: Async: GitLabClient: SessionResource: Logger](
      queriesExecTimes: LabeledHistogram[F]
  ): F[DBMigration[F]] = for {
    accessTokenCrypto         <- AccessTokenCrypto[F]()
    tokenValidator            <- TokenValidator[F]
    tokenRemover              <- TokenRemover[F](queriesExecTimes).pure[F]
    projectAccessTokenCreator <- ProjectAccessTokenCreator[F]()
    associationPersister      <- AssociationPersister[F](queriesExecTimes).pure[F]
  } yield new TokensMigrator[F](accessTokenCrypto,
                                tokenValidator,
                                tokenRemover,
                                projectAccessTokenCreator,
                                associationPersister,
                                queriesExecTimes
  )
}

private class TokensMigrator[F[_]: Async: SessionResource: Logger](
    tokenCrypto:               AccessTokenCrypto[F],
    tokenValidator:            TokenValidator[F],
    tokenRemover:              TokenRemover[F],
    projectAccessTokenCreator: ProjectAccessTokenCreator[F],
    associationPersister:      AssociationPersister[F],
    queriesExecTimes:          LabeledHistogram[F],
    retryInterval:             Duration = 5 seconds
) extends DbClient[F](Some(queriesExecTimes))
    with DBMigration[F]
    with TokenRepositoryTypeSerializers {

  private val logPrefix = "token migration:"

  import associationPersister._
  import fs2.Stream
  import io.renku.db.SqlStatement
  import projectAccessTokenCreator._
  import skunk.Void
  import skunk.implicits._
  import tokenCrypto._
  import tokenValidator._

  override def run(): F[Unit] =
    Stream
      .repeatEval(findTokenWithoutDates)
      .takeThrough(_.nonEmpty)
      .flatMap(maybeProjectAndToken => Stream.emits(maybeProjectAndToken.toList))
      .evalMap { case (proj, encToken) => decryptOrDelete(proj, encToken) }
      .flatMap(maybeProjectAndToken => Stream.emits(maybeProjectAndToken.toList))
      .evalMap { case (proj, token) => deleteWhenInvalidWithRetry(proj, token) }
      .flatMap(maybeProjectAndToken => Stream.emits(maybeProjectAndToken.toList))
      .evalMap { case (proj, token) => createTokenWithRetry(proj, token) }
      .flatMap(maybeProjectAndTokenInfo => Stream.emits(maybeProjectAndTokenInfo.toList))
      .evalMap { case (proj, newTokenInfo) => encrypt(newTokenInfo.token).map(enc => (proj, newTokenInfo, enc)) }
      .evalTap { case (proj, newTokenInfo, encToken) => persistWithRetry(proj, newTokenInfo, encToken) }
      .evalMap { case (proj, _, _) => Logger[F].info(show"$logPrefix $proj token created") }
      .compile
      .drain

  private def findTokenWithoutDates: F[Option[(Project, EncryptedAccessToken)]] = SessionResource[F].useK {
    measureExecutionTime {
      findTokenQuery.build(_.option)
    }
  }

  private def findTokenQuery: SqlStatement.Select[F, Void, (Project, EncryptedAccessToken)] =
    SqlStatement
      .named[F]("find non-migrated token")
      .select[Void, Project ~ EncryptedAccessToken](
        sql"""
        SELECT project_id, project_path, token
        FROM projects_tokens
        WHERE expiry_date IS NULL"""
          .query(projectIdDecoder ~ projectPathDecoder ~ encryptedAccessTokenDecoder)
          .map { case (id: projects.Id) ~ (path: projects.Path) ~ (token: EncryptedAccessToken) =>
            Project(id, path) -> token
          }
      )
      .arguments(Void)

  private def decryptOrDelete(project: Project, encToken: EncryptedAccessToken): F[Option[(Project, AccessToken)]] =
    decrypt(encToken)
      .map(t => Option(project -> t))
      .recoverWith { case ex: Throwable =>
        Logger[F].error(ex)(show"$logPrefix $project token decryption failed; deleting") >>
          tokenRemover.delete(project.id) >>
          Option.empty[(Project, AccessToken)].pure[F]
      }

  private def deleteWhenInvalidWithRetry(project: Project, token: AccessToken): F[Option[(Project, AccessToken)]] = {
    checkValid(token) >>= {
      case true => (project, token).some.pure[F]
      case false =>
        Logger[F].warn(show"$logPrefix $project token invalid; deleting") >>
          tokenRemover.delete(project.id) >>
          Option.empty[(Project, AccessToken)].pure[F]
    }
  }.recoverWith(retry(deleteWhenInvalidWithRetry(project, token))(project))

  private def createTokenWithRetry(project: Project, token: AccessToken): F[Option[(Project, TokenCreationInfo)]] = {
    createPersonalAccessToken(project.id, token) >>= {
      case Some(creationInfo) => (project -> creationInfo).some.pure[F]
      case None =>
        Logger[F].warn(show"$logPrefix $project cannot generate new token; deleting") >>
          tokenRemover.delete(project.id).map(_ => Option.empty[(Project, TokenCreationInfo)])
    }
  }.recoverWith(retry(createTokenWithRetry(project, token))(project))

  private def persistWithRetry(project:        Project,
                               newTokenInfo:   TokenCreationInfo,
                               encryptedToken: EncryptedAccessToken
  ): F[Unit] =
    persistAssociation(TokenStoringInfo(project, encryptedToken, newTokenInfo.dates))
      .recoverWith(retry(persistWithRetry(project, newTokenInfo, encryptedToken))(project))

  private def retry[O](thunk: => F[O])(project: Project): PartialFunction[Throwable, F[O]] = { case ex: Exception =>
    Logger[F].error(ex)(show"$logPrefix $project failure; retrying") >>
      Temporal[F].delayBy(thunk, retryInterval)
  }
}
