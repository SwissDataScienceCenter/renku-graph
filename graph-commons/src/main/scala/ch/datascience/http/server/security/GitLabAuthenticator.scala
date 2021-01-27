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

import cats.data.OptionT
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabApiUrl
import ch.datascience.http.client.{AccessToken, IORestClient}
import io.chrisdavenport.log4cats.Logger
import model._

import scala.concurrent.ExecutionContext

private trait GitLabAuthenticator[Interpretation[_]] {
  def authenticate(accessToken: AccessToken): OptionT[Interpretation, AuthUser]
}

private class GitLabAuthenticatorImpl(
    gitLabApiUrl:            GitLabApiUrl,
    gitLabThrottler:         Throttler[IO, GitLab],
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORestClient(gitLabThrottler, logger)
    with GitLabAuthenticator[IO] {

  import cats.effect._
  import cats.syntax.all._
  import io.circe._
  import org.http4s.Method.GET
  import org.http4s._
  import org.http4s.circe.jsonOf
  import org.http4s.dsl.io._

  override def authenticate(accessToken: AccessToken): OptionT[IO, AuthUser] = OptionT {
    for {
      uri           <- validateUri(s"$gitLabApiUrl/user")
      maybeAuthUser <- send(request(GET, uri, accessToken))(mapResponse(accessToken))
    } yield maybeAuthUser
  }

  private def mapResponse(
      accessToken: AccessToken
  ): PartialFunction[(Status, Request[IO], Response[IO]), IO[Option[AuthUser]]] = {
    case (Ok, _, response) =>
      implicit val entityDecoder: EntityDecoder[IO, AuthUser] = decoder(accessToken)
      response.as[AuthUser].map(Option.apply)
    case (NotFound | Unauthorized | Forbidden, _, _) => None.pure[IO]
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

private object GitLabAuthenticator {

  import ch.datascience.graph.config.GitLabUrl

  def apply(
      gitLabThrottler: Throttler[IO, GitLab],
      logger:          Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[GitLabAuthenticator[IO]] = for {
    gitLabApiUrl <- GitLabUrl[IO]().map(_.apiV4)
  } yield new GitLabAuthenticatorImpl(gitLabApiUrl, gitLabThrottler, logger)
}
