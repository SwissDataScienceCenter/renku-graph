/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

import cats.effect.Async
import cats.effect.kernel.Temporal
import cats.syntax.all._
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.GitLabApiUrl
import io.renku.http.client.{AccessToken, RestClient}
import io.renku.http.server.security.EndpointSecurityException.AuthenticationFailure
import io.renku.http.server.security.model.AuthUser
import io.renku.http.server.security.{Authenticator, EndpointSecurityException}
import org.typelevel.log4cats.Logger

class GitLabAuthenticatorImpl[F[_]: Async: Temporal: Logger](
    gitLabApiUrl:    GitLabApiUrl,
    gitLabThrottler: Throttler[F, GitLab]
) extends RestClient(gitLabThrottler)
    with Authenticator[F] {

  import cats.syntax.all._
  import io.circe._
  import org.http4s.Method.GET
  import org.http4s._
  import org.http4s.circe.jsonOf
  import org.http4s.dsl.io._

  override def authenticate(accessToken: AccessToken): F[Either[EndpointSecurityException, AuthUser]] =
    for {
      uri      <- validateUri(s"$gitLabApiUrl/user")
      authUser <- send(request(GET, uri, accessToken))(mapResponse(accessToken))
    } yield authUser

  private def mapResponse(
      accessToken: AccessToken
  ): PartialFunction[(Status, Request[F], Response[F]), F[
    Either[EndpointSecurityException, AuthUser]
  ]] = {
    case (Ok, _, response) =>
      implicit val entityDecoder: EntityDecoder[F, AuthUser] = decoder(accessToken)
      response.as[AuthUser] map (_.asRight[EndpointSecurityException])
    case (NotFound | Unauthorized | Forbidden, _, _) =>
      AuthenticationFailure.asLeft[AuthUser].leftWiden[EndpointSecurityException].pure[F]
  }

  private def decoder(accessToken: AccessToken): EntityDecoder[F, AuthUser] = {

    import io.renku.graph.model.users
    import io.renku.tinytypes.json.TinyTypeDecoders._

    implicit lazy val userDecoder: Decoder[AuthUser] = { cursor =>
      cursor.downField("id").as[users.GitLabId].map(AuthUser(_, accessToken))
    }

    jsonOf[F, AuthUser]
  }
}

object GitLabAuthenticator {

  def apply[F[_]: Async: Temporal: Logger](
      gitLabThrottler: Throttler[F, GitLab]
  ): F[Authenticator[F]] = for {
    gitLabApiUrl <- GitLabUrlLoader[F]().map(_.apiV4)
  } yield new GitLabAuthenticatorImpl(gitLabApiUrl, gitLabThrottler)
}
