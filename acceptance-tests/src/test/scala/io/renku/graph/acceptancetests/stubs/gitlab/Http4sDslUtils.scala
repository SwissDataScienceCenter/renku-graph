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

import cats.Applicative
import cats.data.OptionT
import cats.effect._
import cats.syntax.all._
import io.renku.graph.acceptancetests.stubs.gitlab.GitLabApiStub.State
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.persons.GitLabId
import io.renku.graph.model.projects.{Id, Path}
import org.http4s.dsl.Http4sDsl
import org.http4s.dsl.impl.OptionalQueryParamDecoderMatcher
import org.http4s.server.middleware.{Logger => LoggerMiddleware}
import org.http4s.{EntityEncoder, HttpApp, HttpRoutes, Response}
import org.typelevel.log4cats.Logger

private[gitlab] trait Http4sDslUtils {

  object GitLabIdVar {
    def unapply(str: String): Option[GitLabId] =
      for {
        idn <- str.toIntOption
        gid <- GitLabId.from(idn).toOption
      } yield gid
  }

  object CommitIdVar {
    def unapply(str: String): Option[CommitId] =
      CommitId.from(str).toOption
  }

  object ProjectPath {
    def unapply(str: String): Option[Path] =
      Path.from(str).toOption
  }

  object ProjectId {
    def unapply(str: String): Option[Id] =
      for {
        idn <- str.toIntOption
        pid <- Id.from(idn).toOption
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

  def enableLogging[F[_]: Async: Logger](app: HttpApp[F]): HttpApp[F] = {
    val logAction: String => F[Unit] = Logger[F].debug(_)
    LoggerMiddleware.httpApp(logHeaders = true, logBody = true, logAction = Some(logAction))(app)
  }

  def withState[F[_]: Sync](stateRef: Ref[F, State])(cont: State => HttpRoutes[F]): HttpRoutes[F] =
    HttpRoutes(req => OptionT.liftF(stateRef.get).flatMap(s => cont(s).run(req)))

  object PageParam extends OptionalQueryParamDecoderMatcher[Int]("page")

  object ActionParam extends OptionalQueryParamDecoderMatcher[String]("action")
}
