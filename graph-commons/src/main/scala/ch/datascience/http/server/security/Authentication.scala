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

package ch.datascience.http.server.security

import cats.MonadError
import cats.data.{Kleisli, OptionT}
import cats.syntax.all._
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import io.chrisdavenport.log4cats.Logger
import model._
import org.http4s.AuthScheme.Bearer
import org.http4s.Credentials.Token
import org.http4s.headers.Authorization
import org.http4s.{AuthedRoutes, Request}

import scala.concurrent.ExecutionContext

private trait Authentication[Interpretation[_]] {
  def authenticate
      : Kleisli[Interpretation, Request[Interpretation], Either[EndpointSecurityException, Option[AuthUser]]]
}

private class AuthenticationImpl[Interpretation[_]](
    authenticator: GitLabAuthenticator[Interpretation]
)(implicit ME:     MonadError[Interpretation, Throwable])
    extends Authentication[Interpretation] {

  import org.http4s.util.CaseInsensitiveString
  import org.http4s.{Header, Request}

  val authenticate
      : Kleisli[Interpretation, Request[Interpretation], Either[EndpointSecurityException, Option[AuthUser]]] =
    Kleisli { request =>
      request.getBearerToken orElse request.getPrivateAccessToken match {
        case Some(token) => authenticator.authenticate(token).map(_.map(Option.apply))
        case None        => Option.empty[AuthUser].asRight[EndpointSecurityException].pure[Interpretation]
      }
    }

  private implicit class RequestOps(request: Request[Interpretation]) {

    lazy val getBearerToken: Option[AccessToken] =
      request.headers.get(Authorization) flatMap {
        case Authorization(Token(Bearer, token)) => OAuthAccessToken(token).some
        case _                                   => None
      }

    lazy val getPrivateAccessToken: Option[AccessToken] =
      request.headers.get(CaseInsensitiveString("PRIVATE-TOKEN")) flatMap {
        case Header(_, token) => PersonalAccessToken(token).some
        case _                => None
      }
  }
}

object Authentication {

  import cats.effect.{ContextShift, IO, Timer}
  import ch.datascience.config.GitLab
  import ch.datascience.control.Throttler
  import org.http4s.server.AuthMiddleware

  def middleware(
      gitLabThrottler: Throttler[IO, GitLab],
      logger:          Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[AuthMiddleware[IO, Option[AuthUser]]] = for {
    authentication <- Authentication(gitLabThrottler, logger)
  } yield middleware(authentication)

  private[security] def middleware(
      authentication: Authentication[IO]
  ): AuthMiddleware[IO, Option[AuthUser]] = {
    val onFailure: AuthedRoutes[EndpointSecurityException, IO] = Kleisli { req =>
      OptionT.some(req.context.toHttpResponse)
    }
    AuthMiddleware(authentication.authenticate, onFailure)
  }

  private def apply(
      gitLabThrottler: Throttler[IO, GitLab],
      logger:          Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[Authentication[IO]] = for {
    authenticator <- GitLabAuthenticator(gitLabThrottler, logger)
  } yield new AuthenticationImpl(authenticator)
}
