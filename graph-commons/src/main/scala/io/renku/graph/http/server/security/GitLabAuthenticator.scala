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

package io.renku.graph.http.server.security

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.http.client.{GitLabClient, UserAccessToken}
import io.renku.http.server.security.EndpointSecurityException.AuthenticationFailure
import io.renku.http.server.security.model.AuthUser
import io.renku.http.server.security.{Authenticator, EndpointSecurityException}
import org.http4s.implicits.http4sLiteralsSyntax
import org.typelevel.log4cats.Logger

class GLAuthenticatorImpl[F[_]: Async: GitLabClient: Logger] extends Authenticator[F] {

  import cats.syntax.all._
  import io.circe._
  import org.http4s._
  import org.http4s.circe.jsonOf
  import org.http4s.dsl.io._

  override def authenticate(accessToken: UserAccessToken): F[Either[EndpointSecurityException, AuthUser]] =
    GitLabClient[F].get(uri"user", "user")(mapResponse(accessToken))(accessToken.some)

  private def mapResponse(
      accessToken: UserAccessToken
  ): PartialFunction[(Status, Request[F], Response[F]), F[Either[EndpointSecurityException, AuthUser]]] = {
    case (Ok, _, response) =>
      implicit val entityDecoder: EntityDecoder[F, AuthUser] = decoder(accessToken)
      response.as[AuthUser] map (_.asRight[EndpointSecurityException])
    case (NotFound | Unauthorized | Forbidden, _, _) =>
      AuthenticationFailure.asLeft[AuthUser].leftWiden[EndpointSecurityException].pure[F]
  }

  private def decoder(accessToken: UserAccessToken): EntityDecoder[F, AuthUser] = {

    import io.renku.graph.model.persons
    import io.renku.tinytypes.json.TinyTypeDecoders._

    implicit lazy val userDecoder: Decoder[AuthUser] = { cursor =>
      cursor.downField("id").as[persons.GitLabId].map(AuthUser(_, accessToken))
    }

    jsonOf[F, AuthUser]
  }
}

object GitLabAuthenticator {
  def apply[F[_]: Async: GitLabClient: Logger]: F[Authenticator[F]] =
    new GLAuthenticatorImpl[F].pure[F].widen[Authenticator[F]]
}
