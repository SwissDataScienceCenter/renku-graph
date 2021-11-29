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

package io.renku.graph.http.server.security

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.http.server.security.Authorizer.{AuthContext, SecurityRecord, SecurityRecordFinder}
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.projects.Visibility.Public
import io.renku.graph.model.users.GitLabId
import io.renku.http.server.security.EndpointSecurityException
import io.renku.http.server.security.EndpointSecurityException.AuthorizationFailure
import io.renku.http.server.security.model.AuthUser
import org.typelevel.log4cats.Logger

trait Authorizer[F[_], Key] {
  def authorize(key: Key, maybeAuthUser: Option[AuthUser]): EitherT[F, EndpointSecurityException, AuthContext[Key]]
}

object Authorizer {
  type SecurityRecord                  = (Visibility, projects.Path, Set[GitLabId])
  type SecurityRecordFinder[F[_], Key] = Key => F[List[SecurityRecord]]

  final case class AuthContext[Key](maybeAuthUser: Option[AuthUser], key: Key, allowedProjects: Set[projects.Path]) {
    def addAllowedProject(path: projects.Path): AuthContext[Key] = copy(allowedProjects = allowedProjects + path)
  }

  def using[F[_]: Async: Logger, Key](
      securityRecordsFinderFactory: F[SecurityRecordFinder[F, Key]]
  ): F[Authorizer[F, Key]] = securityRecordsFinderFactory.map(new AuthorizerImpl[F, Key](_))
}

private class AuthorizerImpl[F[_]: MonadThrow, Key](securityRecordsFinder: SecurityRecordFinder[F, Key])
    extends Authorizer[F, Key] {

  override def authorize(key:           Key,
                         maybeAuthUser: Option[AuthUser]
  ): EitherT[F, EndpointSecurityException, AuthContext[Key]] = for {
    records     <- EitherT.right(securityRecordsFinder(key))
    authContext <- validate(AuthContext[Key](maybeAuthUser, key, Set.empty), records)
  } yield authContext

  private def validate(authContext: AuthContext[Key],
                       records:     List[SecurityRecord]
  ): EitherT[F, EndpointSecurityException, AuthContext[Key]] = EitherT.fromEither[F] {
    records.foldLeft(authContext.asRight[EndpointSecurityException]) {
      case (left @ Left(_), _)             => left
      case (Right(ctx), (Public, path, _)) => (ctx addAllowedProject path).asRight
      case (Right(ctx), (_, path, projectMembers))
          if (projectMembers intersect authContext.maybeAuthUser.map(_.id).toSet).nonEmpty =>
        (ctx addAllowedProject path).asRight
      case _ => AuthorizationFailure.asLeft
    }
  }
}
