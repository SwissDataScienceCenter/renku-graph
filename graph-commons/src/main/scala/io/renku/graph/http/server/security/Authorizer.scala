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

package io.renku.graph.http.server.security

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.http.server.security.Authorizer.{AuthContext, SecurityRecord, SecurityRecordFinder}
import io.renku.graph.model.{persons, projects}
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.projects.Visibility._
import io.renku.http.server.security.EndpointSecurityException
import io.renku.http.server.security.EndpointSecurityException.AuthorizationFailure
import io.renku.http.server.security.model.AuthUser
import org.typelevel.log4cats.Logger

trait Authorizer[F[_], Key] {
  def authorize(key: Key, maybeAuthUser: Option[AuthUser]): EitherT[F, EndpointSecurityException, AuthContext[Key]]
}

object Authorizer {
  final case class SecurityRecord(visibility:     Visibility,
                                  projectSlug:    projects.Slug,
                                  allowedPersons: Set[persons.GitLabId]
  )
  trait SecurityRecordFinder[F[_], Key] extends ((Key, Option[AuthUser]) => F[List[SecurityRecord]]) {
    def asAuthorizer(implicit F: MonadThrow[F]): Authorizer[F, Key] =
      Authorizer.of(this)
  }

  final case class AuthContext[Key](maybeAuthUser: Option[AuthUser], key: Key) {
    def replaceKey[K](key: K): AuthContext[K] = AuthContext(maybeAuthUser, key)
  }

  object AuthContext {
    def forUnknownUser[Key](key: Key): AuthContext[Key] =
      AuthContext(None, key)
  }

  def of[F[_]: MonadThrow, K](securityRecordFinder: SecurityRecordFinder[F, K]): Authorizer[F, K] =
    new AuthorizerImpl[F, K](securityRecordFinder)

  def using[F[_]: Async: Logger, Key](
      securityRecordsFinderFactory: F[SecurityRecordFinder[F, Key]]
  ): F[Authorizer[F, Key]] = securityRecordsFinderFactory.map(new AuthorizerImpl[F, Key](_))
}

private class AuthorizerImpl[F[_]: MonadThrow, Key](securityRecordsFinder: SecurityRecordFinder[F, Key])
    extends Authorizer[F, Key] {

  override def authorize(key:           Key,
                         maybeAuthUser: Option[AuthUser]
  ): EitherT[F, EndpointSecurityException, AuthContext[Key]] = for {
    records     <- EitherT.right(securityRecordsFinder(key, maybeAuthUser))
    authContext <- validate(AuthContext[Key](maybeAuthUser, key), records)
  } yield authContext

  private def validate(authContext: AuthContext[Key],
                       records:     List[SecurityRecord]
  ): EitherT[F, EndpointSecurityException, AuthContext[Key]] = EitherT.fromEither[F] {
    if (isAllowed(authContext, records)) authContext.asRight
    else AuthorizationFailure.asLeft
  }

  private def isAllowed(authContext: AuthContext[Key], records: List[SecurityRecord]): Boolean =
    records.exists(r =>
      r.visibility == Public ||
        (r.visibility == Internal && authContext.maybeAuthUser.isDefined) ||
        (r.visibility == Private && authContext.maybeAuthUser.map(_.id).exists(r.allowedPersons.contains))
    )
}
