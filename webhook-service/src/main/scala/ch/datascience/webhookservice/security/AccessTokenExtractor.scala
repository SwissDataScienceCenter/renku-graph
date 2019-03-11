/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.security

import cats.MonadError
import cats.data.OptionT
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import ch.datascience.http.client.RestClientError.UnauthorizedException
import org.http4s.AuthScheme.Bearer
import org.http4s.Credentials.Token
import org.http4s.headers.Authorization
import org.http4s.util.CaseInsensitiveString
import org.http4s.{Header, Request}

import scala.language.higherKinds

class AccessTokenExtractor[Interpretation[_]](implicit ME: MonadError[Interpretation, Throwable]) {

  def findAccessToken(request: Request[Interpretation]): Interpretation[AccessToken] =
    request.getTokenFromBearer
      .flatMap(toOAuthAccessToken)
      .orElse(request.get("OAUTH-TOKEN") flatMap convert(OAuthAccessToken.from))
      .orElse(request.get("PRIVATE-TOKEN") flatMap convert(PersonalAccessToken.from))
      .getOrElseF(ME.raiseError(UnauthorizedException))

  private lazy val toOAuthAccessToken: Authorization.HeaderT => OptionT[Interpretation, AccessToken] = {
    case Authorization(Token(Bearer, token)) => toOptionT(OAuthAccessToken.from(token))
    case _                                   => OptionT.none
  }

  private def convert(to: String => Either[Exception, AccessToken]): Header => OptionT[Interpretation, AccessToken] = {
    case Header(_, token) => toOptionT(to(token))
    case _                => OptionT.none
  }

  private lazy val toOptionT: Either[Exception, AccessToken] => OptionT[Interpretation, AccessToken] = {
    case Right(accessToken) => OptionT.some(accessToken)
    case Left(_)            => OptionT.none
  }

  private implicit class RequestOps(request: Request[Interpretation]) {

    def get(key: String): OptionT[Interpretation, Header] = OptionT.fromOption {
      request.headers.get(CaseInsensitiveString(key))
    }

    def getTokenFromBearer: OptionT[Interpretation, Authorization.HeaderT] = OptionT.fromOption {
      request.headers.get(Authorization)
    }
  }
}
