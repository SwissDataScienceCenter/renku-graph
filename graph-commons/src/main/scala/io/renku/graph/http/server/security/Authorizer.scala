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
import cats.data.EitherT.{leftT, rightT}
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.http.server.security.Authorizer.{SecurityRecord, SecurityRecordFinder}
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.projects.Visibility.Public
import io.renku.graph.model.users.GitLabId
import io.renku.http.server.security.EndpointSecurityException
import io.renku.http.server.security.EndpointSecurityException.AuthorizationFailure
import io.renku.http.server.security.model.AuthUser
import org.typelevel.log4cats.Logger

trait Authorizer[F[_], Key] {
  def authorize(key: Key, maybeAuthUser: Option[AuthUser]): EitherT[F, EndpointSecurityException, Unit]
}

object Authorizer {
  type SecurityRecord                  = (Visibility, Set[GitLabId])
  type SecurityRecordFinder[F[_], Key] = Key => F[List[SecurityRecord]]

  def using[F[_]: Async: Logger, Key](
      securityRecordsFinderFactory: F[SecurityRecordFinder[F, Key]]
  ): F[Authorizer[F, Key]] = securityRecordsFinderFactory.map(new AuthorizerImpl[F, Key](_))
}

private class AuthorizerImpl[F[_]: MonadThrow, Key](
    securityRecordsFinder: SecurityRecordFinder[F, Key]
) extends Authorizer[F, Key] {

  override def authorize(key: Key, maybeAuthUser: Option[AuthUser]): EitherT[F, EndpointSecurityException, Unit] =
    for {
      records <- EitherT.right(securityRecordsFinder(key))
      _       <- validate(maybeAuthUser, records)
    } yield ()

  private def validate(maybeAuthUser: Option[AuthUser],
                       records:       List[SecurityRecord]
  ): EitherT[F, EndpointSecurityException, Unit] = records -> maybeAuthUser match {
    case (Nil, _)                                                                            => rightT(())
    case ((Public, _) :: Nil, _)                                                             => rightT(())
    case ((_, projectMembers) :: Nil, Some(authUser)) if projectMembers contains authUser.id => rightT(())
    case _ => leftT(AuthorizationFailure)
  }
}
