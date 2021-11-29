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

package io.renku.http.server.security

import cats.data.{Kleisli, OptionT}
import cats.syntax.all._
import cats.{Applicative, MonadThrow}
import io.renku.http.client.AccessToken
import io.renku.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import io.renku.http.server.security.EndpointSecurityException.AuthenticationFailure
import io.renku.http.server.security.model._
import org.http4s.AuthScheme.Bearer
import org.http4s.Credentials.Token
import org.http4s.headers.Authorization
import org.http4s.{AuthedRoutes, Request}
import org.typelevel.ci._

private trait Authentication[F[_]] {
  def authenticateIfNeeded: Kleisli[F, Request[F], Either[EndpointSecurityException, Option[AuthUser]]]
  def authenticate:         Kleisli[F, Request[F], Either[EndpointSecurityException, AuthUser]]
}

private class AuthenticationImpl[F[_]: MonadThrow](authenticator: Authenticator[F]) extends Authentication[F] {

  import org.http4s.{Header, Request}

  override val authenticateIfNeeded: Kleisli[F, Request[F], Either[EndpointSecurityException, Option[AuthUser]]] =
    Kleisli { request =>
      request.getBearerToken orElse request.getPrivateAccessToken match {
        case Some(token) => authenticator.authenticate(token).map(_.map(Option.apply))
        case None        => Option.empty[AuthUser].asRight[EndpointSecurityException].pure[F]
      }
    }

  override val authenticate: Kleisli[F, Request[F], Either[EndpointSecurityException, AuthUser]] =
    Kleisli { request =>
      request.getBearerToken orElse request.getPrivateAccessToken match {
        case Some(token) => authenticator.authenticate(token)
        case None        => (AuthenticationFailure: EndpointSecurityException).asLeft[AuthUser].pure[F]
      }
    }

  private implicit class RequestOps(request: Request[F]) {

    import Header.Select._

    lazy val getBearerToken: Option[AccessToken] =
      request.headers.get(singleHeaders(Authorization.headerInstance)) >>= {
        case Authorization(Token(Bearer, token)) => OAuthAccessToken(token).some
        case _                                   => None
      }

    lazy val getPrivateAccessToken: Option[AccessToken] =
      request.headers.get(ci"PRIVATE-TOKEN").map(_.toList) >>= {
        case Header.Raw(_, token) :: _ => PersonalAccessToken(token).some
        case _                         => None
      }
  }
}

object Authentication {

  import org.http4s.server.AuthMiddleware

  def middlewareAuthenticatingIfNeeded[F[_]: MonadThrow](
      authenticator: Authenticator[F]
  ): F[AuthMiddleware[F, Option[AuthUser]]] = MonadThrow[F].catchNonFatal {
    middlewareAuthenticatingIfNeeded[F](new AuthenticationImpl[F](authenticator))
  }

  private[security] def middlewareAuthenticatingIfNeeded[F[_]: MonadThrow](
      authentication: Authentication[F]
  ): AuthMiddleware[F, Option[AuthUser]] =
    AuthMiddleware[F, EndpointSecurityException, Option[AuthUser]](authentication.authenticateIfNeeded, onFailure)

  def middleware[F[_]: MonadThrow](
      authenticator: Authenticator[F]
  ): F[AuthMiddleware[F, AuthUser]] = MonadThrow[F].catchNonFatal {
    middleware(new AuthenticationImpl(authenticator))
  }

  private[security] def middleware[F[_]: MonadThrow](
      authentication: Authentication[F]
  ): AuthMiddleware[F, AuthUser] =
    AuthMiddleware[F, EndpointSecurityException, AuthUser](authentication.authenticate, onFailure)

  private def onFailure[F[_]: Applicative]: AuthedRoutes[EndpointSecurityException, F] = Kleisli { req =>
    OptionT.some[F](req.context.toHttpResponse)
  }
}
