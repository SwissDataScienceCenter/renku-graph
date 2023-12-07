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

package io.renku.graph.acceptancetests.stubs.gitlab

import cats.Applicative
import cats.data.{Kleisli, OptionT}
import cats.effect._
import cats.syntax.all._
import io.renku.graph.acceptancetests.stubs.gitlab.GitLabApiStub.State
import io.renku.graph.model.{persons, projects}
import io.renku.http.client.AccessToken._
import io.renku.http.client.{AccessToken, ProjectAccessTokenDefaultPrefix, UserAccessToken}
import org.http4s.headers.Authorization
import org.http4s.server.AuthMiddleware
import org.http4s.{Request, _}
import org.typelevel.ci._

private[gitlab] object GitLabAuth {
  import GitLabStateQueries.{findAuthedProject, findAuthedUser}

  sealed trait AuthedReq extends Product with Serializable {
    type AT <: AccessToken
    def accessToken: AT
  }

  object AuthedReq {
    final case class AuthedUser(userId: persons.GitLabId, accessToken: UserAccessToken) extends AuthedReq {
      type AT = UserAccessToken
    }
    final case class AuthedProject(projectId:   projects.GitLabId,
                                   userId:      persons.GitLabId,
                                   accessToken: ProjectAccessToken
    ) extends AuthedReq {
      type AT = ProjectAccessToken
    }
  }

  def auth[F[_]: Async](state: State)(cont: AuthedReq => HttpRoutes[F]): HttpRoutes[F] = {

    val authedReq: Kleisli[F, Request[F], Either[String, AuthedReq]] = Kleisli { req =>
      maybeProjectAuthedReq(req, state) orElse maybeUserAuthedReq(req, state) match {
        case None => "No token provided in request".asLeft[AuthedReq].pure[F]
        case someAuthReq @ Some(_) =>
          someAuthReq
            .toRight("User or Project unknown")
            .pure[F]
            .widen
      }
    }
    val middleware = AuthMiddleware(authedReq, authFail(Status.Unauthorized))
    middleware(AuthedRoutes(authReq => cont(authReq.context).run(authReq.req)))
  }

  def authF[F[_]: Async](stateRef: Ref[F, State])(cont: AuthedReq => HttpRoutes[F]): HttpRoutes[F] =
    Kleisli(req => OptionT.liftF(stateRef.get).flatMap(auth(_)(cont).run(req)))

  def authOpt[F[_]: Async](state: State)(cont: Option[AuthedReq] => HttpRoutes[F]): HttpRoutes[F] = {
    val authedReq: Kleisli[F, Request[F], Either[String, Option[AuthedReq]]] = Kleisli { req =>
      maybeProjectAuthedReq(req, state) orElse maybeUserAuthedReq(req, state) match {
        case None => Option.empty[AuthedReq].asRight[String].pure[F]
        case someAuthReq @ Some(_) =>
          someAuthReq
            .map(_.some)
            .toRight("User or Project unknown")
            .pure[F]
            .widen
      }
    }
    val middleware = AuthMiddleware(authedReq, authFail(Status.Forbidden))
    middleware(AuthedRoutes(authReq => cont(authReq.context).run(authReq.req)))
  }

  def authOptF[F[_]: Async](stateRef: Ref[F, State])(cont: Option[AuthedReq] => HttpRoutes[F]): HttpRoutes[F] =
    Kleisli(req => OptionT.liftF(stateRef.get).flatMap(authOpt(_)(cont).run(req)))

  def apply(user: AuthedReq): Header.ToRaw =
    user.accessToken match {
      case t: ProjectAccessToken   => Header.ToRaw.modelledHeadersToRaw(t.asInstanceOf[ProjectAccessToken])
      case t: UserOAuthAccessToken => Header.ToRaw.modelledHeadersToRaw(t.asInstanceOf[UserOAuthAccessToken])
      case t: PersonalAccessToken  => Header.ToRaw.modelledHeadersToRaw(t.asInstanceOf[PersonalAccessToken])
    }

  private def maybeUserAuthedReq[F[_]](req: Request[F], state: State) =
    (req.headers.get[UserOAuthAccessToken] orElse req.headers.get[PersonalAccessToken])
      .flatMap(findAuthedUser(_)(state))

  private def maybeProjectAuthedReq[F[_]](req: Request[F], state: State) =
    req.headers.get[ProjectAccessToken] >>= (findAuthedProject(_)(state))

  private implicit val privateTokenHeader: Header[PersonalAccessToken, Header.Single] =
    Header.create(
      ci"PRIVATE-TOKEN",
      _.value,
      value => PersonalAccessToken.from(value).leftMap(ex => ParseFailure(ex.getMessage, ""))
    )

  private implicit val userOAuthAccessToken: Header[UserOAuthAccessToken, Header.Single] =
    bearerToken(v => !ProjectAccessTokenDefaultPrefix.exists(v), UserOAuthAccessToken(_: String))

  private implicit val projectAccessToken: Header[ProjectAccessToken, Header.Single] =
    bearerToken(ProjectAccessTokenDefaultPrefix.exists, ProjectAccessToken(_: String))

  private def bearerToken[T <: AccessToken](predicate: String => Boolean,
                                            factory:   String => T
  ): Header[T, Header.Single] = {
    val h: Header[Authorization, Header.Single] = Header[Authorization]
    Header.create(
      h.name,
      v => h.value(Authorization(Credentials.Token(AuthScheme.Bearer, v.value))),
      (h.parse _).andThen(
        _.flatMap(header =>
          header.credentials match {
            case Credentials.Token(AuthScheme.Bearer, token) if predicate(token) => factory(token).asRight
            case _ => Left(ParseFailure("Invalid token", ""))
          }
        )
      )
    )
  }

  private def authFail[F[_]: Applicative](status: Status): AuthedRoutes[String, F] = Kleisli { req =>
    OptionT.liftF(Response[F](status).withEntity(req.context).pure[F])
  }
}
