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
import cats.data.OptionT
import cats.effect._
import cats.syntax.all._
import io.circe.Encoder
import io.renku.graph.acceptancetests.stubs.gitlab.GitLabApiStub.State
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.{persons, projects}
import org.http4s.circe.CirceEntityCodec._
import org.http4s.dsl.Http4sDsl
import org.http4s.dsl.impl.OptionalQueryParamDecoderMatcher
import org.http4s.server.middleware.{Logger => LoggerMiddleware}
import org.http4s.{EntityEncoder, Header, HttpApp, HttpRoutes, Request, Response}
import org.typelevel.ci._
import org.typelevel.log4cats.Logger

private[gitlab] trait Http4sDslUtils {

  object UserGitLabId {
    def unapply(str: String): Option[persons.GitLabId] =
      for {
        idn <- str.toIntOption
        gid <- persons.GitLabId.from(idn).toOption
      } yield gid
  }

  object CommitIdVar {
    def unapply(str: String): Option[CommitId] =
      CommitId.from(str).toOption
  }

  object ProjectPath {
    def unapply(str: String): Option[projects.Path] =
      projects.Path.from(str).toOption
  }

  object ProjectId {
    def unapply(str: String): Option[projects.GitLabId] =
      for {
        idn <- str.toIntOption
        pid <- projects.GitLabId.from(idn).toOption
      } yield pid
  }

  object ProjectAccessTokenId {
    def unapply(str: String): Option[Int] = str.toIntOption
  }

  def OkOrNotFound[F[_]: Applicative, A](payload: Option[A])(implicit enc: EntityEncoder[F, A]): F[Response[F]] = {
    val dsl = new Http4sDsl[F] {}
    import dsl._

    payload.map(Ok(_)).getOrElse(Response.notFound[F].pure[F])
  }

  def OkWithTotalHeader[F[_]: Applicative, A](
      req: Request[F]
  )(entities: List[A])(implicit enc: Encoder[A]): F[Response[F]] = {
    val dsl = new Http4sDsl[F] {}
    import dsl._

    val perPage    = req.params.getOrElse("per_page", "20").toInt
    val totalPages = (entities.size / perPage) + (if (entities.size % perPage == 0) 0 else 1)

    Ok(entities).map(_.withHeaders(Header.Raw(ci"X-Total-Pages", totalPages.toString)))
  }

  def enableLogging[F[_]: Async: Logger](app: HttpApp[F]): HttpApp[F] = {
    val logAction: String => F[Unit] = Logger[F].debug(_)
    LoggerMiddleware.httpApp(logHeaders = true, logBody = true, logAction = Some(logAction))(app)
  }

  def withState[F[_]: Sync](stateRef: Ref[F, State])(cont: State => HttpRoutes[F]): HttpRoutes[F] =
    HttpRoutes(req => OptionT.liftF(stateRef.get).flatMap(s => cont(s).run(req)))

  object PageParam extends OptionalQueryParamDecoderMatcher[Int]("page")

  object ActionParam extends OptionalQueryParamDecoderMatcher[String]("action")
}
