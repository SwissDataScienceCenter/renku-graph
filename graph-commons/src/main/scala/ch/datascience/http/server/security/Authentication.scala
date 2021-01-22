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
import io.chrisdavenport.log4cats.Logger
import model._
import org.http4s.headers.Authorization

import scala.concurrent.ExecutionContext

private class Authentication[Interpretation[_]](
    authenticator: GitLabAuthenticator[Interpretation]
)(implicit ME:     MonadError[Interpretation, Throwable]) {
  import org.http4s.util.CaseInsensitiveString
  import org.http4s.{Header, Request}

  val authenticate: Kleisli[OptionT[Interpretation, *], Request[Interpretation], Option[AuthUser]] = Kleisli {
    request =>
      request.getBearerToken orElse request.getPrivateAccessToken match {
        case Some(authHeader) => authenticator.authenticate(authHeader) map Option.apply
        case None             => OptionT.some(Option.empty[AuthUser])
      }
  }

  private implicit class RequestOps(request: Request[Interpretation]) {

    lazy val getBearerToken: Option[Header] =
      request.headers.get(Authorization)

    lazy val getPrivateAccessToken: Option[Header] =
      request.headers.get(CaseInsensitiveString("PRIVATE-TOKEN"))
  }
}

object Authentication {

  import cats.effect.{ContextShift, IO, Timer}
  import ch.datascience.config.GitLab
  import ch.datascience.control.Throttler
  import org.http4s.server.AuthMiddleware

  def middlewareWithFallThrough(
      gitLabThrottler: Throttler[IO, GitLab],
      logger:          Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[AuthMiddleware[IO, Option[AuthUser]]] = for {
    authentication <- Authentication(gitLabThrottler, logger)
  } yield AuthMiddleware.withFallThrough(authentication.authenticate)

  private def apply(
      gitLabThrottler: Throttler[IO, GitLab],
      logger:          Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[Authentication[IO]] = for {
    authenticator <- GitLabAuthenticator(gitLabThrottler, logger)
  } yield new Authentication(authenticator)
}
