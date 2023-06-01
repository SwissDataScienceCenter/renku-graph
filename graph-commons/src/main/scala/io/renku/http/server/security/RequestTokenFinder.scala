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

package io.renku.http.server.security

import cats.syntax.all._
import io.renku.http.client.AccessToken.{PersonalAccessToken, UserOAuthAccessToken}
import io.renku.http.client.UserAccessToken
import org.http4s.AuthScheme.Bearer
import org.http4s.Credentials.Token
import org.http4s.Header.Select._
import org.http4s.headers.Authorization
import org.http4s.{Header, Request}
import org.typelevel.ci._

object RequestTokenFinder {

  def getAccessToken[F[_]](request: Request[F]): Option[UserAccessToken] =
    getBearerToken(request) orElse getPrivateAccessToken(request)

  private def getBearerToken[F[_]](request: Request[F]): Option[UserAccessToken] =
    request.headers.get(singleHeaders(Authorization.headerInstance)) >>= {
      case Authorization(Token(Bearer, token)) => UserOAuthAccessToken(token).some
      case _                                   => None
    }

  private def getPrivateAccessToken[F[_]](request: Request[F]): Option[UserAccessToken] =
    request.headers.get(ci"PRIVATE-TOKEN").map(_.toList) >>= {
      case Header.Raw(_, token) :: _ => PersonalAccessToken(token).some
      case _                         => None
    }
}
