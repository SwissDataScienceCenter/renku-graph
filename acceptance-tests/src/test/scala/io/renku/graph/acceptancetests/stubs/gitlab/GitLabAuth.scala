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

package io.renku.graph.acceptancetests.stubs.gitlab

import cats.data.{Kleisli, OptionT}
import cats.effect._
import cats.syntax.all._
import io.renku.graph.acceptancetests.stubs.gitlab.GitLabApiStub.State
import io.renku.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import io.renku.http.server.security.model.AuthUser
import org.http4s._
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.Authorization
import org.http4s.server.AuthMiddleware
import org.typelevel.ci._
import StateSyntax._

object GitLabAuth {
  def auth[F[_]: Async](state: State)(cont: AuthUser => HttpRoutes[F]): HttpRoutes[F] = {
    val dsl = new Http4sDsl[F] {}
    import dsl._

    val authUser: Kleisli[F, Request[F], Either[String, AuthUser]] = Kleisli { req =>
      req.headers.get[PersonalAccessToken].orElse(req.headers.get[OAuthAccessToken]) match {
        case None => "No token provided in request".asLeft[AuthUser].pure[F]
        case Some(token) =>
          state.findUserByToken(token).toRight("User not found").pure[F]
      }
    }
    val authFail: AuthedRoutes[String, F] = Kleisli(req => OptionT.liftF(Forbidden(req.context)))

    val middleware = AuthMiddleware(authUser, authFail)
    middleware(AuthedRoutes(authReq => cont(authReq.context).run(authReq.req)))
  }

  def apply(user: AuthUser): Header.ToRaw =
    user.accessToken match {
      case t: PersonalAccessToken => Header.ToRaw.modelledHeadersToRaw(t)
      case t: OAuthAccessToken    => Header.ToRaw.modelledHeadersToRaw(t)
    }

  implicit val privateTokenHeader: Header[PersonalAccessToken, Header.Single] =
    Header.create(
      ci"PRIVATE-TOKEN",
      _.value,
      value => PersonalAccessToken.from(value).leftMap(ex => ParseFailure(ex.getMessage, ""))
    )

  implicit val oauthAccessToken: Header[OAuthAccessToken, Header.Single] = {
    val h: Header[Authorization, Header.Single] = Header[Authorization]
    Header.create(
      h.name,
      v => h.value(Authorization(Credentials.Token(AuthScheme.Bearer, v.value))),
      (h.parse _).andThen(
        _.flatMap(header =>
          header.credentials match {
            case Credentials.Token(AuthScheme.Bearer, token) => OAuthAccessToken(token).asRight
            case _                                           => Left(ParseFailure("Invalid token", ""))
          }
        )
      )
    )
  }
}
