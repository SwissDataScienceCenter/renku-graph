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
import io.renku.graph.http.server.security.Authorizer.SecurityRecord
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
  type SecurityRecord = (Visibility, Set[GitLabId])
}

class AuthorizerImpl[F[_]: MonadThrow, Key](
    findSecurityRecord: Key => F[List[SecurityRecord]]
) extends Authorizer[F, Key] {

  override def authorize(path: Key, maybeAuthUser: Option[AuthUser]): EitherT[F, EndpointSecurityException, Unit] =
    for {
      records <- EitherT.right(findSecurityRecord(path))
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
