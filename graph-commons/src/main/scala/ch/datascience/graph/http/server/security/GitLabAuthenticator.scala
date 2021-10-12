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

package ch.datascience.graph.http.server.security

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabUrlLoader
import ch.datascience.graph.model.GitLabApiUrl
import ch.datascience.http.client.{AccessToken, RestClient}
import ch.datascience.http.server.security.EndpointSecurityException.AuthenticationFailure
import ch.datascience.http.server.security.model.AuthUser
import ch.datascience.http.server.security.{Authenticator, EndpointSecurityException}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

class GitLabAuthenticatorImpl(
    gitLabApiUrl:            GitLabApiUrl,
    gitLabThrottler:         Throttler[IO, GitLab],
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends RestClient(gitLabThrottler, logger)
    with Authenticator[IO] {

  import cats.effect._
  import cats.syntax.all._
  import io.circe._
  import org.http4s.Method.GET
  import org.http4s._
  import org.http4s.circe.jsonOf
  import org.http4s.dsl.io._

  override def authenticate(accessToken: AccessToken): IO[Either[EndpointSecurityException, AuthUser]] = for {
    uri      <- validateUri(s"$gitLabApiUrl/user")
    authUser <- send(request(GET, uri, accessToken))(mapResponse(accessToken))
  } yield authUser

  private def mapResponse(
      accessToken: AccessToken
  ): PartialFunction[(Status, Request[IO], Response[IO]), IO[Either[EndpointSecurityException, AuthUser]]] = {
    case (Ok, _, response) =>
      implicit val entityDecoder: EntityDecoder[IO, AuthUser] = decoder(accessToken)
      response.as[AuthUser] map (Right(_))
    case (NotFound | Unauthorized | Forbidden, _, _) => Left(AuthenticationFailure).pure[IO]
  }

  private def decoder(accessToken: AccessToken): EntityDecoder[IO, AuthUser] = {

    import ch.datascience.graph.model.users
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    implicit lazy val userDecoder: Decoder[AuthUser] = { cursor =>
      cursor.downField("id").as[users.GitLabId].map(AuthUser(_, accessToken))
    }

    jsonOf[IO, AuthUser]
  }
}

object GitLabAuthenticator {

  def apply(
      gitLabThrottler: Throttler[IO, GitLab],
      logger:          Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[Authenticator[IO]] = for {
    gitLabApiUrl <- GitLabUrlLoader[IO]().map(_.apiV4)
  } yield new GitLabAuthenticatorImpl(gitLabApiUrl, gitLabThrottler, logger)
}
