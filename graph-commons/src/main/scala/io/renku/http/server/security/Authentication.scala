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

import cats.data.{Kleisli, OptionT}
import cats.syntax.all._
import cats.{Applicative, MonadThrow}
import io.renku.http.server.security.model._
import org.http4s.{AuthedRoutes, Request}

private trait Authentication[F[_]] {
  def authenticateIfNeeded: Kleisli[F, Request[F], Either[EndpointSecurityException, MaybeAuthUser]]
  def authenticate:         Kleisli[F, Request[F], Either[EndpointSecurityException, AuthUser]]
}

private class AuthenticationImpl[F[_]: MonadThrow](authenticator: Authenticator[F]) extends Authentication[F] {

  import RequestTokenFinder._
  import org.http4s.Request

  override val authenticateIfNeeded: Kleisli[F, Request[F], Either[EndpointSecurityException, MaybeAuthUser]] =
    Kleisli { request =>
      getAccessToken(request) match {
        case Some(token) => authenticator.authenticate(token).map(_.map(MaybeAuthUser.apply))
        case None        => MaybeAuthUser.noUser.asRight[EndpointSecurityException].pure[F]
      }
    }

  override val authenticate: Kleisli[F, Request[F], Either[EndpointSecurityException, AuthUser]] =
    authenticateIfNeeded.map(_.flatMap(_.required))
}

object Authentication {

  import org.http4s.server.AuthMiddleware

  def middlewareAuthenticatingIfNeeded[F[_]: MonadThrow](
      authenticator: Authenticator[F]
  ): F[AuthMiddleware[F, MaybeAuthUser]] = MonadThrow[F].catchNonFatal {
    middlewareAuthenticatingIfNeeded[F](new AuthenticationImpl[F](authenticator))
  }

  private[security] def middlewareAuthenticatingIfNeeded[F[_]: MonadThrow](
      authentication: Authentication[F]
  ): AuthMiddleware[F, MaybeAuthUser] =
    AuthMiddleware[F, EndpointSecurityException, MaybeAuthUser](authentication.authenticateIfNeeded, onFailure)

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
